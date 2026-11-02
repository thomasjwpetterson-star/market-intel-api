"""Read-only company activity for saved-company feeds; no notification delivery."""
import datetime,hashlib,re
from .financial_readiness import guard,validated
from .work_vocabulary import plain


class ActivityFeedMixin:
    def activity_feed(self,cage,limit=25,offset=0,since=None):
        if not re.fullmatch('[A-Z0-9]{5}',cage) or type(limit) is not int or not 1<=limit<=100 or type(offset) is not int or not 0<=offset<=1000000:
            raise ValueError('Invalid company activity request')
        if since is not None:
            if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',since):raise ValueError('Use an ISO activity date')
            datetime.date.fromisoformat(since)
        m=self.manifest();company='CAGE:'+cage;parts=[]
        if 'company_activity_actions' in m['tables']:
            parts.append("SELECT 'prime_action' activity_kind,transaction_key source_record_id,action_date activity_date,work_text title,customer buyer_name,action_obligation observed_amount,'net_action_obligation' amount_basis,source_url,evidence_id,award_key,cast(NULL AS varchar) parent_award_key FROM "+self.table('company_activity_actions')+" WHERE company_id='"+company+"'")
        if 'company_subcontract_facts' in m['tables']:
            # Canonical values are available only after clean-source validation.
            amount='reported_amount' if validated(m) else 'cast(NULL AS double)'
            parts.append("SELECT 'subcontract_report' activity_kind,report_key source_record_id,event_date activity_date,work_text title,prime_name buyer_name,"+amount+" observed_amount,'reported_subcontract_package_amount' amount_basis,source_url,evidence_id,cast(NULL AS varchar) award_key,parent_award_key FROM "+self.table('company_subcontract_facts')+" WHERE company_id='"+company+"'")
        result=dict(records=[],company_id=company,release=m['run_id'],as_of=m['as_of'],has_more=False,next_offset=None,availability='available' if parts else 'not_published',coverage='Held contract actions and subcontract reports; source dates, not first-seen dates. Item sales, platform activity and email delivery are separate.')
        if not parts:return result
        sql='SELECT * FROM ('+' UNION ALL '.join(parts)+") activity WHERE try_cast(substr(activity_date,1,10) AS date)<=DATE '"+m['as_of']+"'"
        if since:sql+=" AND try_cast(substr(activity_date,1,10) AS date)>=DATE '"+since+"'"
        sql+=' ORDER BY activity_date DESC,activity_kind,source_record_id OFFSET '+str(offset)+' LIMIT '+str(limit+1)
        rows=self.rows(sql);result.update(has_more=len(rows)>limit,next_offset=offset+limit if len(rows)>limit else None)
        for raw in rows[:limit]:
            row=dict(raw);row['title']=plain(row.get('title'))
            # Stable through release changes; a corrected source row keeps its
            # source identity. This is not an invented procurement event.
            row['activity_id']=hashlib.sha256((company+'|'+row['activity_kind']+'|'+str(row['source_record_id'])).encode()).hexdigest()
            row['activity_label']='Contract action' if row['activity_kind']=='prime_action' else 'Subcontract report'
            row['interpretation']='Observed action obligation, including funding changes.' if row['activity_kind']=='prime_action' else 'Reported subcontract history; not a new invitation to bid.'
            if row['activity_kind']=='subcontract_report' and not validated(m):row['observed_amount']=None
            result['records'].append(row)
        return result
