import gspread
import requests
import os
import json
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from datetime import datetime, timedelta

# ── ✏️ 여기만 수정하세요 ───────────────────────────
SHEET_ID        = '1GeQctImT_N_C_BZ1cOOcy0T5Dg3zEIgf_p5i8B1WqqU'   # 내부용 구글 시트 ID
SHEET_ID_AGENCY = '18lc2b5XH1qCxSyzE_KkaFUPyjwYooLyOlDsfq0nEzs4'   # 대행사용 구글 시트 ID
BQ_PROJECT      = 'mrtdata'                                           # BigQuery 프로젝트 ID
# ─────────────────────────────────────────────────

SCOPES_SHEETS   = ['https://www.googleapis.com/auth/spreadsheets',
                   'https://www.googleapis.com/auth/drive']
BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
CREDS_FILE      = os.path.join(BASE_DIR, 'credentials.json')
TOKEN_FILE      = os.path.join(BASE_DIR, 'token.json')
BQ_TOKEN_FILE   = os.path.join(BASE_DIR, 'bq_token.json')

# ── BigQuery 인증 ──────────────────────────────────
def get_bq_creds():
    with open(BQ_TOKEN_FILE) as f:
        t = json.load(f)
    creds = Credentials(
        token=t['token'], refresh_token=t['refresh_token'],
        token_uri=t['token_uri'], client_id=t['client_id'],
        client_secret=t['client_secret'], scopes=t['scopes']
    )
    creds.refresh(Request())
    # 갱신된 토큰 저장
    t['token'] = creds.token
    with open(BQ_TOKEN_FILE, 'w') as f:
        json.dump(t, f)
    return creds

def bq_query(creds, sql):
    """BigQuery 쿼리 실행 (startIndex 기반 페이지네이션)"""
    import time
    headers = {'Authorization': f'Bearer {creds.token}', 'Content-Type': 'application/json'}
    post_url = f'https://bigquery.googleapis.com/bigquery/v2/projects/{BQ_PROJECT}/queries'
    body = {'query': sql, 'useLegacySql': False, 'timeoutMs': 60000, 'maxResults': 0}
    res = requests.post(post_url, headers=headers, json=body)
    data = res.json()
    if 'error' in data:
        raise Exception(f'BQ 오류: {data["error"]}')

    job_ref = data.get('jobReference', {})
    job_id = job_ref.get('jobId', '')
    location = job_ref.get('location', '')
    get_url = f'https://bigquery.googleapis.com/bigquery/v2/projects/{BQ_PROJECT}/queries/{job_id}'

    # 쿼리 완료 대기 (jobComplete=False인 경우)
    while not data.get('jobComplete', True):
        time.sleep(3)
        data = requests.get(get_url, headers=headers,
                            params={'maxResults': 0, 'location': location}).json()
        if 'error' in data:
            raise Exception(f'BQ 오류: {data["error"]}')

    schema = [f['name'] for f in data['schema']['fields']]
    total_rows = int(data.get('totalRows', 0))
    print(f'    BQ totalRows: {total_rows}')

    # startIndex 기반 페이지네이션 (pageToken 미반환 이슈 방지)
    rows = []
    start_index = 0
    page_size = 10000
    while start_index < total_rows:
        params = {'maxResults': page_size, 'startIndex': start_index, 'location': location}
        resp = requests.get(get_url, headers=headers, params=params).json()
        if 'error' in resp:
            raise Exception(f'BQ 페이지네이션 오류: {resp["error"]}')
        page_rows = resp.get('rows', [])
        if not page_rows:
            print(f'    ⚠️ 빈 페이지 수신 (startIndex={start_index})')
            break
        rows += page_rows
        start_index += len(page_rows)
        if start_index < total_rows:
            print(f'    진행: {start_index}/{total_rows}행...')

    print(f'    실제 수신: {len(rows)}행')
    if len(rows) != total_rows:
        print(f'    ⚠️ 경고: totalRows({total_rows})와 실제 수신({len(rows)})이 다름')

    return [{col: (v['v'] if v['v'] is not None else None)
             for col, v in zip(schema, row['f'])} for row in rows]

# ── BQ 쿼리: 파워링크 ──────────────────────────────
SQL_POWERLINK = """
WITH naver AS (
    SELECT
        basis_dt, campaign_name, adgroup_name, nccKeywordId, keyword, nccAdgroupId, device,
        SUM(salesAmt) AS cost
    FROM `mrtdata.edw.ST_ADS_STAT_NAVER_KEYWORD`
    WHERE basis_dt >= '{start_date}' AND basis_dt <= '{end_date}'
      AND basis_dt != CURRENT_DATE()
      AND keyword IS NOT NULL
      AND campaign_name != '브랜드검색'
    GROUP BY 1,2,3,4,5,6,7
),
cm AS (
    SELECT resve_id, SUM(CON_MARGIN) AS CON_MARGIN
    FROM `mrtdata.edw_fpna.MART_FPNA_NONAIR_PROFIT_D`
    GROUP BY 1
),
purchase AS (
    SELECT ms.BASIS_DATE, RESVE_N_KEYWORD_ID AS n_keyword_id,
        SUM(ms.SALES_KRW_PRICE) AS gmv,
        SUM(c.CON_MARGIN) AS con_margin
    FROM `mrtdata.edw_mart.MART_SALE_D` AS ms
    LEFT JOIN cm AS c ON ms.RESVE_ID = c.resve_id
    WHERE ms.BASIS_DATE BETWEEN '{start_date}' AND '{end_date}'
      AND ms.kind = 1 AND RESVE_UTM_SOURCE IN ('NAD')
    GROUP BY 1,2
)
SELECT
    DATE_TRUNC(n.basis_dt, DAY) AS basis_dt,
    campaign_name,
    adgroup_name AS adset_name,
    keyword AS ad_name,
    SUM(n.cost) AS cost,
    SUM(IFNULL(p.gmv, 0)) AS gmv,
    SUM(IFNULL(p.con_margin, 0)) AS con_margin
FROM naver n
LEFT JOIN purchase p ON n.nccKeywordId = p.n_keyword_id AND n.basis_dt = p.BASIS_DATE
WHERE n.cost > 0
GROUP BY 1,2,3,4
ORDER BY 1 DESC
"""

# ── BQ 쿼리: 쇼검광 ────────────────────────────────
SQL_SHOPPING = """
WITH pf AS (
    SELECT
        basis_dt, campaign_name, adgroup_name, device, nccAdId,
        SUM(salesAmt) AS cost
    FROM `mrtdata.edw.ST_ADS_STAT_NAVER_AD`
    WHERE basis_dt >= '{start_date}' AND basis_dt <= '{end_date}'
      AND basis_dt != CURRENT_DATE()
    GROUP BY 1,2,3,4,5
),
str AS (
    SELECT ad_id, product_id_of_mall, product_name
    FROM `mrtdata.edw.DW_ADS_STAT_NAVER_MASTER_REPORT_SHOPPING_PRODUCT_AD`
),
cm AS (
    SELECT resve_id, SUM(CON_MARGIN) AS CON_MARGIN
    FROM `mrtdata.edw_fpna.MART_FPNA_NONAIR_PROFIT_D`
    GROUP BY 1
),
purchase AS (
    SELECT ms.BASIS_DATE, RESVE_N_AD AS n_ad,
        SUM(ms.SALES_KRW_PRICE) AS gmv,
        SUM(c.CON_MARGIN) AS con_margin
    FROM `mrtdata.edw_mart.MART_SALE_D` AS ms
    LEFT JOIN cm AS c ON ms.RESVE_ID = c.resve_id
    WHERE ms.BASIS_DATE BETWEEN '{start_date}' AND '{end_date}'
      AND ms.kind = 1 AND RESVE_UTM_SOURCE IN ('NaverShopping')
      AND RESVE_N_CAMPAIGN_TYPE = '2'
    GROUP BY 1,2
)
SELECT
    DATE_TRUNC(pf.basis_dt, DAY) AS basis_dt,
    pf.campaign_name,
    pf.adgroup_name,
    str.product_id_of_mall,
    str.product_name,
    SUM(pf.cost) AS cost,
    SUM(IFNULL(p.gmv, 0)) AS gmv,
    SUM(IFNULL(p.con_margin, 0)) AS con_margin
FROM pf
LEFT JOIN str ON str.ad_id = pf.nccAdId
LEFT JOIN purchase p ON p.n_ad = pf.nccAdId AND p.BASIS_DATE = pf.basis_dt
WHERE pf.cost > 0
GROUP BY 1,2,3,4,5
ORDER BY 1 DESC
"""

# ── Google Sheets 인증 ─────────────────────────────
def get_gspread_client():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES_SHEETS)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(TOKEN_FILE, 'w') as f:
                f.write(creds.to_json())
        else:
            raise Exception('Google Sheets 토큰 만료 - token.json 갱신 필요')
    return gspread.authorize(creds)

# ── CM 판정 ────────────────────────────────────────
def get_cm_result(cm_roas):
    if cm_roas >= 200: return 'CM ROAS 200% 이상'
    if cm_roas >= 100: return 'CM ROAS 100% 이상'
    if cm_roas >= 50:  return 'CM ROAS 100% 미만'
    return 'CM ROAS 50% 미만'

def get_signal(cm_roas):
    if cm_roas >= 200: return '🟢'
    if cm_roas >= 100: return '🔵'
    if cm_roas >= 50:  return '🟡'
    return '🔴'

# ── adset_name 파싱 ────────────────────────────────
def parse_adset(adset_name, campaign_name):
    parts   = (adset_name or '').split('_')
    country = parts[1] if len(parts) > 1 else ''
    city    = parts[2] if len(parts) > 2 else ''
    segment = parts[3] if len(parts) > 3 else ''
    if segment == '민박':         vertical = '민박'
    elif segment == '상품명':     vertical = '투어티켓'
    elif '-TA' in campaign_name:  vertical = '투어티켓'
    elif '-AC' in campaign_name:  vertical = '해외숙소'
    elif '-UC' in campaign_name:  vertical = '여행기타'
    elif '-FL' in campaign_name:  vertical = '항공'
    elif '-PK' in campaign_name:  vertical = '패키지'
    else:                         vertical = ''
    return country, city, vertical

# ── 주차 레이블 ────────────────────────────────────
def get_week_label(date_str):
    try:
        d = datetime.strptime(str(date_str)[:10], '%Y-%m-%d')
        monday = d - timedelta(days=d.weekday())
        sunday = monday + timedelta(days=6)
        return f"{monday.strftime('%Y-%m/%d')}~{sunday.strftime('%m/%d')}"
    except:
        return ''

# ── 시트 쓰기 (청크) ───────────────────────────────
def write_chunks(ws, data, chunk=2000):
    import time
    def safe(v):
        if v is None: return ''
        if isinstance(v, float) and v != v: return ''
        return v
    safe_data = [[safe(c) for c in row] for row in data]
    ws.clear()
    for i in range(0, len(safe_data), chunk):
        for attempt in range(5):
            try:
                ws.update(safe_data[i:i+chunk], f'A{i+1}')
                time.sleep(1.5)
                break
            except Exception as e:
                if attempt == 4: raise
                print(f'    재시도 {attempt+1}/5: {e}')
                time.sleep(10 * (attempt + 1))

LEGEND = [['신호', '의미'],
          ['🟢', '성과 매우 우수 + 상향 조정 등 적극 액션 필요'],
          ['🔵', '성과 준수 + 상향 조정'],
          ['🟡', '성과 미달'],
          ['🔴', '성과 매우 저조 + 즉각 하향 조정 필요']]

# ── 시트 업데이트 ──────────────────────────────────
def update_sheet(gc, sheet_base, rows, ad_name_field, include_product=False):
    wb        = gc.open_by_key(SHEET_ID)
    wb_agency = gc.open_by_key(SHEET_ID_AGENCY)

    cols_in = 11 if include_product else 10
    cols_ag = 8  if include_product else 7

    try:    ws_in = wb.worksheet(sheet_base + '_내부')
    except: ws_in = wb.add_worksheet(sheet_base + '_내부', rows=5000, cols=cols_in)
    try:    ws_ag = wb.worksheet(sheet_base)
    except: ws_ag = wb.add_worksheet(sheet_base, rows=5000, cols=cols_ag)
    try:    ws_ag2 = wb_agency.worksheet(sheet_base)
    except: ws_ag2 = wb_agency.add_worksheet(sheet_base, rows=5000, cols=cols_ag)

    if include_product:
        in_headers = ['날짜', '캠페인명', '그룹명', '상품ID', '상품명', '광고비', 'GMV', '공헌이익', 'CM ROAS(%)', '신호', 'CM 결과']
        ag_headers = ['날짜', '캠페인명', '그룹명', '상품ID', '상품명', '광고비', 'GMV', '신호']
    else:
        in_headers = ['날짜', '캠페인명', '그룹명', '키워드', '광고비', 'GMV', '공헌이익', 'CM ROAS(%)', '신호', 'CM 결과']
        ag_headers = ['날짜', '캠페인명', '그룹명', '키워드', '광고비', 'GMV', '신호']

    in_data, ag_data = [in_headers], [ag_headers]

    for row in rows:
        adset      = row.get(ad_name_field, '') or ''
        campaign   = row.get('campaign_name', '') or ''
        cost       = float(row.get('cost', 0) or 0)
        gmv        = float(row.get('gmv',  0) or 0)
        con_margin = float(row.get('con_margin', 0) or 0)
        cm_roas    = round(con_margin / cost * 100, 1) if cost > 0 else 0
        signal     = get_signal(cm_roas)
        cm_result  = get_cm_result(cm_roas)
        basis_dt   = str(row.get('basis_dt', ''))[:10]

        if include_product:
            product_id   = row.get('product_id_of_mall', '') or ''
            product_name = row.get('product_name', '') or ''
            in_data.append([basis_dt, campaign, adset, product_id, product_name,
                            round(cost), round(gmv), round(con_margin), cm_roas, signal, cm_result])
            ag_data.append([basis_dt, campaign, adset, product_id, product_name,
                            round(cost), round(gmv), signal])
        else:
            keyword = row.get('ad_name', '') or ''
            in_data.append([basis_dt, campaign, adset, keyword,
                            round(cost), round(gmv), round(con_margin), cm_roas, signal, cm_result])
            ag_data.append([basis_dt, campaign, adset, keyword,
                            round(cost), round(gmv), signal])

    write_chunks(ws_in, in_data)
    write_chunks(ws_ag, ag_data)
    write_chunks(ws_ag2, ag_data)

    legend_col  = 'J1' if include_product else 'I1'
    resize_cols = 11  if include_product else 10
    ws_ag2.resize(cols=resize_cols)
    ws_ag2.update(LEGEND, legend_col)
    print(f'  {sheet_base} 완료 - {len(rows)}행')

# ── 주차별 시트 업데이트 (대행사용) ───────────────────
def update_weekly_sheet(gc, sheet_base, rows, ad_name_field, include_product=False):
    wb_agency = gc.open_by_key(SHEET_ID_AGENCY)

    weekly = {}
    for row in rows:
        adset      = row.get(ad_name_field, '') or ''
        campaign   = row.get('campaign_name', '') or ''
        cost       = float(row.get('cost', 0) or 0)
        gmv        = float(row.get('gmv',  0) or 0)
        con_margin = float(row.get('con_margin', 0) or 0)
        week_label = get_week_label(row.get('basis_dt', ''))

        if include_product:
            key = (week_label, campaign, adset,
                   str(row.get('product_id_of_mall', '') or ''),
                   row.get('product_name', '') or '')
        else:
            key = (week_label, campaign, adset, row.get('ad_name', '') or '')

        if key not in weekly:
            weekly[key] = {'cost': 0, 'gmv': 0, 'con_margin': 0}
        weekly[key]['cost']       += cost
        weekly[key]['gmv']        += gmv
        weekly[key]['con_margin'] += con_margin

    if include_product:
        ag_headers = ['주차', '캠페인명', '그룹명', '상품ID', '상품명', '광고비', 'GMV', '신호']
    else:
        ag_headers = ['주차', '캠페인명', '그룹명', '키워드', '광고비', 'GMV', '신호']

    ag_data = [ag_headers]
    for key in sorted(weekly.keys(), reverse=True):
        v          = weekly[key]
        cost       = v['cost']
        con_margin = v['con_margin']
        cm_roas    = round(con_margin / cost * 100, 1) if cost > 0 else 0
        signal     = get_signal(cm_roas)

        if include_product:
            week_label, campaign, adset, product_id, product_name = key
            ag_data.append([week_label, campaign, adset, product_id, product_name,
                            round(cost), round(v['gmv']), signal])
        else:
            week_label, campaign, adset, keyword = key
            ag_data.append([week_label, campaign, adset, keyword,
                            round(cost), round(v['gmv']), signal])

    tab_name    = sheet_base + '_주차별 CM 성과'
    legend_col  = 'J1' if include_product else 'I1'
    resize_cols = 11  if include_product else 10

    try:    ws = wb_agency.worksheet(tab_name)
    except: ws = wb_agency.add_worksheet(tab_name, rows=2000, cols=9)

    ws.clear()
    ws.update(ag_data, 'A1')
    ws.resize(cols=resize_cols)
    ws.update(LEGEND, legend_col)
    print(f'  {tab_name} 완료 - {len(ag_data)-1}행')

# ── 메인 ───────────────────────────────────────────
def main():
    today      = datetime.today()
    start_date = (today - timedelta(days=90)).strftime('%Y-%m-%d')
    end_date   = (today - timedelta(days=1)).strftime('%Y-%m-%d')

    print('BigQuery 인증 중...')
    bq_creds = get_bq_creds()

    print(f'파워링크 BQ 쿼리 실행 중... ({start_date} ~ {end_date})')
    rows_pl = bq_query(bq_creds, SQL_POWERLINK.format(start_date=start_date, end_date=end_date))
    print(f'  파워링크 {len(rows_pl)}행 수신')

    print('쇼검광 BQ 쿼리 실행 중...')
    rows_sh = bq_query(bq_creds, SQL_SHOPPING.format(start_date=start_date, end_date=end_date))
    print(f'  쇼검광 {len(rows_sh)}행 수신')

    print('Google Sheets 인증 중...')
    gc = get_gspread_client()

    print('Google Sheets 업데이트 중...')
    update_sheet(gc, '파워링크', rows_pl, 'adset_name')
    update_weekly_sheet(gc, '파워링크', rows_pl, 'adset_name')

    update_sheet(gc, '쇼검광', rows_sh, 'adgroup_name', include_product=True)
    update_weekly_sheet(gc, '쇼검광', rows_sh, 'adgroup_name', include_product=True)

    print('전체 완료:', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

if __name__ == '__main__':
    main()
