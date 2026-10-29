import asyncio
import copy
import hashlib
import json
import time
import re
import unicodedata
from collections import OrderedDict
from datetime import datetime, timezone
from uuid import uuid4
import httpx
from .access import AccessError
from .models import Query, Result, Source

PUBLIC_FIELDS = {
    "company": "name cage uei entity_id location description time_period ultimate_parent_name ultimate_parent_uei is_parent prime_exposure sub_exposure downstream_subcontract_value top_capabilities capabilities_hidden top_platforms platforms_hidden network_partners network_hidden prime_customers prime_customers_hidden government_customers government_customers_hidden subcontractors subcontractors_hidden top_nsns nsns_hidden top_contract_id top_contract_desc top_contract_value mapped_platform_value platform_mapping_coverage platform_share_basis prime_customer_share_basis government_customer_share_basis subcontractor_share_basis",
    "nsn": "associated_part_number_count part_numbers_hidden approved_sources_hidden supplier_sites_hidden platforms_hidden is_multi_platform observed_contract_count contracts_hidden item_name nsn niin fsc_code part_numbers associated_supplier_site_count approved_source supplier_sites platforms recent_contracts logistics_summary demand_supply_teaser opportunity_summary observed_price_summary latest_price average_price price_history time_period",
    "platform": "entity_id slug name time_period total_obligations contract_count contractor_count top_vendors top_agencies parts part_count recent_awards",
    "award": "entity_id contract_id vendor_name vendor_cage agency description latest_action_description platform_family observed_net_obligations latest_reported_total_obligated latest_reported_current_value latest_reported_potential_value annual_obligations start_date last_action_date first_year last_year action_count",
    "search": "query entries",
}
ROUTES = {"company": "companies", "nsn": "nsn", "platform": "platforms", "award": "awards"}
WEB_ROUTES = {"company": "intelligence/companies", "nsn": "intelligence/nsn", "platform": "intelligence/platforms", "award": "intelligence/awards"}

class PublicBackend:
    def __init__(self, client, origin):
        if not origin.startswith("https://"):
            raise ValueError("Backend must use HTTPS")
        self.client, self.origin = client, origin.rstrip("/")

    async def fetch(self, query):
        path = "/api/public/intelligence/"
        if query.kind == "search":
            path += "search"
            params = {"q": query.identifier, "limit": query.limit}
        else:
            path += f"{ROUTES[query.kind]}/{query.identifier}"
            params = {"public_schema": "3"} if query.kind in {"company", "nsn", "award"} else {}
        try:
            # Streaming enforces byte limits before decoding; no redirects or retries.
            async with self.client.stream("GET", self.origin + path, params=params, timeout=8, follow_redirects=False) as response:
                if response.status_code == 404:
                    raise AccessError("not_found", "No public Mimir record was found.", 404)
                response.raise_for_status()
                data = bytearray()
                async for chunk in response.aiter_bytes():
                    data.extend(chunk)
                    if len(data) > 262144:
                        raise AccessError("result_too_large", "Source record exceeds the external response limit.", 502)
                result = json.loads(data)
                if not isinstance(result, dict):
                    raise ValueError("Expected public record")
                return result
        except (httpx.HTTPError, ValueError):
            raise AccessError("source_unavailable", "Mimir source data is temporarily unavailable.", 503) from None

class Audit:
    """Bounded aggregate counts only: no prompts, tokens, subjects or record payloads."""
    def __init__(self):
        self.counts = {}

    def record(self, channel, operation, status, authenticated):
        key = (channel, operation, status, authenticated)
        self.counts[key] = self.counts.get(key, 0) + 1

    def snapshot(self):
        return [{"channel": k[0], "operation": k[1], "status": k[2], "authenticated": k[3], "count": v} for k, v in self.counts.items()]

class Service:
    def __init__(self, backend, resolver, ledger=None, audit=None, max_concurrency=4):
        self.backend, self.resolver, self.ledger = backend, resolver, ledger
        self.audit = audit or Audit()
        self.capacity = asyncio.Semaphore(max_concurrency)
        self.cache = OrderedDict()
        self.minute = OrderedDict()

    async def query(self, query: Query, authorization=None, channel="api") -> Result:
        request_id = str(uuid4())
        identity = None
        reserved = False
        try:
            identity = await self.resolver.resolve(authorization)
            if self.ledger is None:
                raise AccessError("accounting_not_configured", "Shared Mimir usage accounting is not connected.", 503)
            # In addition to shared daily/monthly credits: per-subject burst bound.
            subject = hashlib.sha256(identity.subject.encode()).hexdigest()
            now = time.monotonic()
            bucket, count = self.minute.get(subject, (now, 0))
            if now - bucket >= 60:
                bucket, count = now, 0
            if count >= 10:
                raise AccessError("rate_limit", "Please wait before requesting more research.", 429)
            self.minute[subject] = bucket, count + 1
            self.minute.move_to_end(subject)
            if len(self.minute) > 10000:
                self.minute.popitem(last=False)
            await self.ledger.reserve(request_id, identity, f"distribution:{query.kind}")
            reserved = True
            key = query.model_dump_json()
            cached = self.cache.get(key)
            if cached and now - cached[0] < 300:
                payload, retrieved_at = copy.deepcopy(cached[1]), cached[2]
                self.cache.move_to_end(key)
            else:
                try:
                    await asyncio.wait_for(self.capacity.acquire(), timeout=0.1)
                except TimeoutError:
                    raise AccessError("busy", "Research capacity is busy; retry shortly.", 503) from None
                try:
                    payload = await asyncio.wait_for(self.backend.fetch(query), timeout=10)
                    retrieved_at = datetime.now(timezone.utc)
                finally:
                    self.capacity.release()
                self.cache[key] = (now, copy.deepcopy(payload), retrieved_at)
                self.cache.move_to_end(key)
                if len(self.cache) > 256:
                    self.cache.popitem(last=False)
            truncated = False
            def bounded(value, depth=0):
                nonlocal truncated
                if depth > 8:
                    truncated = True
                    return None
                if isinstance(value, list):
                    truncated |= len(value) > query.limit
                    return [bounded(v, depth+1) for v in value[:query.limit]]
                if isinstance(value, dict):
                    return {str(k): bounded(v, depth+1) for k, v in value.items() if not any(s in str(k).lower() for s in ("secret", "token", "credential", "email", "sql"))}
                if isinstance(value, str) and len(value) > 3000:
                    truncated = True
                    return value[:3000]
                return value
            data = bounded({k: payload[k] for k in PUBLIC_FIELDS[query.kind].split() if k in payload})
            if query.kind == "search":
                url = "https://www.mimiradvisors.org/ask-mimir"
            else:
                url = f"https://www.mimiradvisors.org/{WEB_ROUTES[query.kind]}/{query.identifier}"
            if query.kind == "company":
                slug_text = " ".join(str(payload.get(k) or "") for k in ("name", "location"))
                slug_text = unicodedata.normalize("NFKD", slug_text).encode("ascii", "ignore").decode().lower().replace("&", " and ")
                slug = re.sub(r"[^a-z0-9]+", "-", slug_text).strip("-")[:90] or "defense-company"
                url += "/" + slug
            result = Result(request_id=request_id, kind=query.kind, identifier=query.identifier, data=data,
                sources=[Source(url=url, retrieved_at=retrieved_at, published_at=payload.get("public_last_modified") or payload.get("published_at"))], truncated=truncated,
                warnings=["Public snapshot only; hidden results are excluded. Observed obligations are not revenue or forecasts.", "Retrieved time is not source-data freshness; unavailable publication dates remain null.", "Platform associations may be broad or shared; verify award descriptions before treating any mapped supplier as a confirmed program supplier."])
            if len(result.model_dump_json().encode()) > 65536:
                raise AccessError("result_too_large", "Bounded result is too large.", 502)
            await self.ledger.finish(request_id, True)
            reserved = False
            self.audit.record(channel, query.kind, "success", identity.authenticated)
            return result
        except BaseException as exc:
            if reserved:
                await asyncio.shield(self.ledger.finish(request_id, False))
            status = exc.code if isinstance(exc, AccessError) else "source_unavailable"
            self.audit.record(channel, query.kind, status, bool(identity and identity.authenticated))
            if isinstance(exc, (AccessError, asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                raise
            raise AccessError("source_unavailable", "Research could not be completed.", 503) from None
