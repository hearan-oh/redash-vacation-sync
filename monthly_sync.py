"""
월별 CM 신호 시트 동기화 (대행사용 전용).

대행사 시트에 (월 × 캠페인 × 그룹 × 키워드/상품) 단위 신호만 표시.
일자별 raw는 sync.py가 별도로 담당. 이 스크립트는 BQ에서 월 단위 GROUP BY로
직접 집계해 받기 때문에 가벼움.

정책 (C):
  - 현재 월        : 매일 덮어쓰기
  - 전월           : today.day <= 5 인 동안만 덮어쓰기 (월마감 보강 기간)
  - 그 외 과거 월  : 시트에 있으면 보존, 없으면 풀 데이터 1회만 백필
  - BQ 윈도우 시작이 부분월(예: 1/27부터)이면 그 월은 백필 제외
"""
import gspread
import requests
import os
import json
import time
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from datetime import datetime, timedelta

# ── ✏️ 여기만 수정하세요 ───────────────────────────
SHEET_ID_AGENCY = '18lc2b5XH1qCxSyzE_KkaFUPyjwYooLyOlDsfq0nEzs4'
BQ_PROJECT      = 'mrtdata'
LOOKBACK_DAYS   = 90  # BQ 조회 윈도우
# ─────────────────────────────────────────────────

SCOPES_SHEETS = ['https://www.googleapis.com/auth/spreadsheets',
                 'https://www.googleapis.com/auth/drive']
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
CREDS_FILE    = os.path.join(BASE_DIR, 'credentials.json')
TOKEN_FILE    = os.path.join(BASE_DIR, 'token.json')
BQ_TOKEN_FILE = os.path.join(BASE_DIR, 'bq_token.json')

# ── 인증 ────────────────────────────────────────────
def get_bq_creds():
    with open(BQ_TOKEN_FILE) as f:
        t = json.load(f)
    creds = Credentials(
        token=t['token'], refresh_token=t['refresh_token'],
        token_uri=t['token_uri'], client_id=t['client_id'],
        client_secret=t['client_secret'], scopes=t['scopes']
    )
    creds.refresh(Request())
    t['token'] = creds.token
    with open(BQ_TOKEN_FILE, 'w') as f:
        json.dump(t, f)
    return creds

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

# ── BQ 쿼리 실행 ───────────────────────────────────
def bq_query(creds, sql):
    headers  = {'Authorization': f'Bearer {creds.token}', 'Content-Type': 'application/json'}
    post_url = f'https://bigquery.googleapis.com/bigquery/v2/projects/{BQ_PROJECT}/queries'
    body     = {'query': sql, 'useLegacySql': False, 'timeoutMs': 60000, 'maxResults': 0}
    res      = requests.post(post_url, headers=headers, json=body)
    data     = res.json()
    if 'error' in data:
        raise Exception(f'BQ 오류: {data["error"]}')

    job_ref  = data.get('jobReference', {})
    job_id   = job_ref.get('jobId', '')
    location = job_ref.get('location', '')
    get_url  = f'https://bigquery.googleapis.com/bigquery/v2/projects/{BQ_PROJECT}/queries/{job_id}'

    while not data.get('jobComplete', True):
        time.sleep(3)
        data = requests.get(get_url, headers=headers,
                            params={'maxResults': 0, 'location': location}).json()
        if 'error' in data:
            raise Exception(f'BQ 오류: {data["error"]}')

    schema     = [f['name'] for f in data['schema']['fields']]
    total_rows = int(data.get('totalRows', 0))
    print(f'    BQ totalRows: {total_rows}')

    rows        = []
    start_index = 0
    page_size   = 10000
    while start_index < total_rows:
        params = {'maxResults': page_size, 'startIndex': start_index, 'location': location}
        resp   = requests.get(get_url, headers=headers, params=params).json()
        if 'error' in resp:
            raise Exception(f'BQ 페이지네이션 오류: {resp["error"]}')
        page_rows = resp.get('rows', [])
        if not page_rows:
            break
        rows        += page_rows
        start_index += len(page_rows)

    return [{col: (v['v'] if v['v'] is not None else None)
             for col, v in zip(schema, row['f'])} for row in rows]

# ── BQ 쿼리: 월별 집계 ─────────────────────────────
SQL_POWERLINK_MONTHLY = """
WITH naver AS (
    SELECT
        DATE_TRUNC(basis_dt, MONTH) AS month_dt,
        campaign_name, adgroup_name, nccKeywordId, keyword,
        SUM(salesAmt) AS cost
    FROM `mrtdata.edw.ST_ADS_STAT_NAVER_KEYWORD`
    WHERE basis_dt >= '{start_date}' AND basis_dt <= '{end_date}'
      AND basis_dt != CURRENT_DATE()
      AND keyword IS NOT NULL
      AND campaign_name != '브랜드검색'
    GROUP BY 1,2,3,4,5
),
cm AS (
    SELECT resve_id, SUM(CON_MARGIN) AS CON_MARGIN
    FROM `mrtdata.edw_fpna.MART_FPNA_NONAIR_PROFIT_D`
    GROUP BY 1
),
purchase AS (
    SELECT DATE_TRUNC(ms.BASIS_DATE, MONTH) AS month_dt,
        RESVE_N_KEYWORD_ID AS n_keyword_id,
        SUM(ms.SALES_KRW_PRICE) AS gmv,
        SUM(c.CON_MARGIN) AS con_margin
    FROM `mrtdata.edw_mart.MART_SALE_D` AS ms
    LEFT JOIN cm AS c ON ms.RESVE_ID = c.resve_id
    WHERE ms.BASIS_DATE BETWEEN '{start_date}' AND '{end_date}'
      AND ms.kind = 1 AND RESVE_UTM_SOURCE IN ('NAD')
    GROUP BY 1,2
)
SELECT
    FORMAT_DATE('%Y-%m', n.month_dt) AS month_label,
    n.campaign_name,
    n.adgroup_name AS adset_name,
    n.keyword AS ad_name,
    SUM(n.cost) AS cost,
    SUM(IFNULL(p.gmv, 0)) AS gmv,
    SUM(IFNULL(p.con_margin, 0)) AS con_margin
FROM naver n
LEFT JOIN purchase p
  ON n.nccKeywordId = p.n_keyword_id AND n.month_dt = p.month_dt
WHERE n.cost > 0
GROUP BY 1,2,3,4
ORDER BY 1 DESC
"""

SQL_SHOPPING_MONTHLY = """
WITH pf AS (
    SELECT
        DATE_TRUNC(basis_dt, MONTH) AS month_dt,
        campaign_name, adgroup_name, nccAdId,
        SUM(salesAmt) AS cost
    FROM `mrtdata.edw.ST_ADS_STAT_NAVER_AD`
    WHERE basis_dt >= '{start_date}' AND basis_dt <= '{end_date}'
      AND basis_dt != CURRENT_DATE()
    GROUP BY 1,2,3,4
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
    SELECT DATE_TRUNC(ms.BASIS_DATE, MONTH) AS month_dt,
        RESVE_N_AD AS n_ad,
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
    FORMAT_DATE('%Y-%m', pf.month_dt) AS month_label,
    pf.campaign_name,
    pf.adgroup_name,
    str.product_id_of_mall,
    str.product_name,
    SUM(pf.cost) AS cost,
    SUM(IFNULL(p.gmv, 0)) AS gmv,
    SUM(IFNULL(p.con_margin, 0)) AS con_margin
FROM pf
LEFT JOIN str ON str.ad_id = pf.nccAdId
LEFT JOIN purchase p ON p.n_ad = pf.nccAdId AND p.month_dt = pf.month_dt
WHERE pf.cost > 0
GROUP BY 1,2,3,4,5
ORDER BY 1 DESC
"""

# ── 신호 ────────────────────────────────────────────
def get_signal(cm_roas):
    if cm_roas >= 200: return '🟢'
    if cm_roas >= 100: return '🔵'
    if cm_roas >= 50:  return '🟡'
    return '🔴'

LEGEND = [['신호', '의미'],
          ['🟢', '성과 매우 우수 + 상향 조정 등 적극 액션 필요'],
          ['🔵', '성과 준수 + 상향 조정'],
          ['🟡', '성과 미달'],
          ['🔴', '성과 매우 저조 + 즉각 하향 조정 필요']]

# ── 시트 업데이트 ──────────────────────────────────
def update_monthly_sheet(gc, sheet_base, rows, start_date, include_product=False):
    wb_agency = gc.open_by_key(SHEET_ID_AGENCY)
    today          = datetime.today()
    current_month  = today.strftime('%Y-%m')
    prev_month     = (today.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
    refresh_months = {current_month}
    if today.day <= 5:
        refresh_months.add(prev_month)

    window_start_dt    = datetime.strptime(start_date, '%Y-%m-%d')
    window_start_month = window_start_dt.strftime('%Y-%m')
    window_start_full  = (window_start_dt.day == 1)

    if include_product:
        ag_headers = ['월', '캠페인명', '그룹명', '상품ID', '상품명', '광고비', 'GMV', '신호']
    else:
        ag_headers = ['월', '캠페인명', '그룹명', '키워드', '광고비', 'GMV', '신호']

    tab_name    = sheet_base + '_월별 CM 성과'
    cols        = 8 if include_product else 7
    legend_col  = 'J1' if include_product else 'I1'
    resize_cols = 11  if include_product else 10

    try:
        ws = wb_agency.worksheet(tab_name)
        existing = ws.get_all_values()
    except:
        ws = wb_agency.add_worksheet(tab_name, rows=2000, cols=cols)
        existing = []

    # 백필 대상 결정
    bq_months       = {str(r.get('month_label', '') or '') for r in rows}
    bq_months.discard('')
    existing_months = {r[0] for r in existing[1:] if r and r[0]} if existing and len(existing) > 1 else set()
    backfill        = bq_months - existing_months
    if not window_start_full:
        backfill.discard(window_start_month)
    refresh_months |= backfill

    # 갱신 대상 월의 새 행 (BQ가 이미 월 단위로 집계됨 → row 1개 = 시트 1행)
    new_rows = []
    for row in rows:
        month_label = str(row.get('month_label', '') or '')
        if month_label not in refresh_months:
            continue
        cost       = float(row.get('cost', 0) or 0)
        gmv        = float(row.get('gmv',  0) or 0)
        con_margin = float(row.get('con_margin', 0) or 0)
        cm_roas    = round(con_margin / cost * 100, 1) if cost > 0 else 0
        signal     = get_signal(cm_roas)
        campaign   = row.get('campaign_name', '') or ''

        if include_product:
            adset        = row.get('adgroup_name', '') or ''
            product_id   = str(row.get('product_id_of_mall', '') or '')
            product_name = row.get('product_name', '') or ''
            new_rows.append([month_label, campaign, adset, product_id, product_name,
                             round(cost), round(gmv), signal])
        else:
            adset   = row.get('adset_name', '') or ''
            keyword = row.get('ad_name', '') or ''
            new_rows.append([month_label, campaign, adset, keyword,
                             round(cost), round(gmv), signal])

    # 보존 대상 행 (refresh 대상 월이 아닌 기존 시트 행)
    preserved = []
    if existing and len(existing) > 1:
        for r in existing[1:]:
            if r and r[0] and r[0] not in refresh_months:
                preserved.append(r)

    all_rows = preserved + new_rows
    def _cost(r):
        try:    return float(r[5]) if len(r) > 5 else 0
        except: return 0
    all_rows.sort(key=lambda r: (r[0], _cost(r)), reverse=True)

    final = [ag_headers] + all_rows
    ws.clear()
    ws.update(final, 'A1')
    ws.resize(cols=resize_cols)
    ws.update(LEGEND, legend_col)
    print(f'  {tab_name} 완료 - 갱신 {len(new_rows)}행 / 보존 {len(preserved)}행 (refresh: {sorted(refresh_months)})')

# ── 메인 ───────────────────────────────────────────
def main():
    today      = datetime.today()
    start_date = (today - timedelta(days=LOOKBACK_DAYS)).strftime('%Y-%m-%d')
    end_date   = (today - timedelta(days=1)).strftime('%Y-%m-%d')

    print('BigQuery 인증 중...')
    bq_creds = get_bq_creds()

    print(f'파워링크 월별 BQ 쿼리 실행 중... ({start_date} ~ {end_date})')
    rows_pl = bq_query(bq_creds, SQL_POWERLINK_MONTHLY.format(start_date=start_date, end_date=end_date))
    print(f'  파워링크 월별 {len(rows_pl)}행 수신')

    print('쇼검광 월별 BQ 쿼리 실행 중...')
    rows_sh = bq_query(bq_creds, SQL_SHOPPING_MONTHLY.format(start_date=start_date, end_date=end_date))
    print(f'  쇼검광 월별 {len(rows_sh)}행 수신')

    print('Google Sheets 인증 중...')
    gc = get_gspread_client()

    print('월별 시트 업데이트 중...')
    update_monthly_sheet(gc, '파워링크', rows_pl, start_date)
    update_monthly_sheet(gc, '쇼검광',   rows_sh, start_date, include_product=True)

    print('전체 완료:', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

if __name__ == '__main__':
    main()
