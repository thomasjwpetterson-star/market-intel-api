"""Trusted-adapter-only private pilot. Uses the existing Ask state database."""
import hmac
import os
from pathlib import Path
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from research_sessions import ResearchSessions, SessionConflict, SessionLimit


def create_research_session_router(state_path, resolve_access):
    router=APIRouter()

    @router.post('/api/research-session')
    async def research_session(request: Request):
        if os.getenv('ASK_MIMIR_RESEARCH_SESSIONS_ENABLED')!='1':
            raise HTTPException(404,'Not found')
        secret=os.getenv('ASK_MIMIR_TRUSTED_PROXY_SECRET','')
        supplied=request.headers.get('x-ask-mimir-proxy-secret','')
        if not secret or not hmac.compare_digest(secret.encode(),supplied.encode()):
            raise HTTPException(401,'Verified Mimir access is required')
        access=resolve_access(request)
        allowed={x.strip() for x in os.getenv('ASK_MIMIR_SESSION_SUBJECT_ALLOWLIST','').split(',') if x.strip()}
        if not access.authenticated or access.subject_id not in allowed:
            raise HTTPException(403,'Private research-session access is not enabled')
        length=request.headers.get('content-length','0')
        if not length.isdigit(): raise HTTPException(400,'Check the research-session request')
        if int(length)>2048:
            raise HTTPException(413,'Request is too large')
        raw=await request.body()
        if len(raw)>2048: raise HTTPException(413,'Request is too large')
        import json
        try:
            payload=json.loads(raw)
            if not isinstance(payload,dict) or set(payload)-{'action','request_key','fingerprint'}:
                raise ValueError()
            action=payload.get('action')
            if action not in {'status','admit','succeed','fail'}: raise ValueError()
            store=ResearchSessions(Path(state_path()))
            if action=='status':
                if set(payload)!={'action'}: raise ValueError()
                result={'usage':store.status(access.subject_id,access.tier)}
            elif action=='admit':
                result=store.admit(access.subject_id,access.tier,payload.get('request_key'),payload.get('fingerprint'))
            else:
                if set(payload)!={'action','request_key'} or not isinstance(payload.get('request_key'),str): raise ValueError()
                result={'usage':store.finish(access.subject_id,access.tier,payload['request_key'],success=action=='succeed')}
        except SessionLimit as exc:
            return JSONResponse({'error':{'code':exc.code},'usage':exc.usage},status_code=429,headers={'Cache-Control':'private, no-store'})
        except SessionConflict:
            raise HTTPException(409,'Research reservation could not be verified')
        except (ValueError,TypeError):
            raise HTTPException(400,'Check the research-session request')
        return JSONResponse(result,headers={'Cache-Control':'private, no-store'})

    return router
