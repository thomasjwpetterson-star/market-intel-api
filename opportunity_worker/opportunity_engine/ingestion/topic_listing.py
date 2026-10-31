"""Bounded complete SBA listing reads when equal-sort pages overlap."""
import re,urllib.parse

def collect(fetch,url,max_pages=40,max_passes=3):
 links=set();pages=[];expected=None
 for attempt in range(max_passes):
  current_url=url;seen=set()
  for index in range(max_pages):
   if current_url in seen:raise ValueError('Topic pagination loop')
   seen.add(current_url);soup,sha,path=fetch(current_url);text=soup.get_text(' ',strip=True)
   match=re.search(r'Showing\s+[0-9,]+[-–][0-9,]+\s+of\s+([0-9,]+)\s+results',text)
   total=int(match.group(1).replace(',','')) if match else None
   if total is None or (expected is not None and expected!=total):raise ValueError('Topic result population changed or is undeclared')
   expected=total
   found={urllib.parse.urljoin(current_url,a['href']) for a in soup.select('h3 a[href]') if re.fullmatch(r'/topics/\d+',a['href'])}
   links.update(found);pages.append(dict(url=current_url,sha256=sha,path=path,expected_results=expected,listing_pass=attempt+1,topic_urls=sorted(found)))
   if len(links)>expected:raise ValueError('Topic population changed during pagination')
   next_link=soup.select_one('a[rel="next"]')
   if not next_link:
    selected=soup.select_one('a[aria-current="page"]');number=int(urllib.parse.parse_qs(urllib.parse.urlparse(selected['href']).query).get('page',['0'])[0]) if selected else index
    next_link=next((a for a in soup.select('a[href]') if urllib.parse.parse_qs(urllib.parse.urlparse(a['href']).query).get('page')==[str(number+1)]),None)
   if not next_link:break
   current_url=urllib.parse.urljoin(current_url,next_link['href'])
  else:raise ValueError('Topic pagination exceeded the source bound')
  if len(links)==expected:return links,pages,expected
 # A reread can resolve overlapping equally-sorted pages, but never waive the
 # declared distinct-topic count. Every observed listing body remains archived.
 raise ValueError('Incomplete topic-list coverage after bounded rereads')
