import requests
import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os
from datetime import datetime, timedelta

# ── 설정 ──────────────────────────────────────────
REDASH_BASE = 'https://redash.myrealtrip.com'
API_KEY     = 'UIygrE9LGBrx5dqHAZmNrO0yKOltYxrPdEynnzfK'
SHEET_ID         = '1GeQctImT_N_C_BZ1cOOcy0T5Dg3zEIgf_p5i8B1WqqU'  # 내부용
SHEET_ID_AGENCY  = '18lc2b5XH1qCxSyzE_KkaFUPyjwYooLyOlDsfq0nEzs4'  # 대행사용
SCOPES      = ['https://www.googleapis.com/auth/spreadsheets',
               'https://www.googleapis.com/auth/drive']
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
CREDS_FILE  = os.path.join(BASE_DIR, 'credentials.json')
TOKEN_FILE  = os.path.join(BASE_DIR, 'token.json')

# ── Google 인증 ────────────────────────────────────
def get_gspread_client():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, 'w') as f:
            f.write(creds.to_json())
    return gspread.authorize(creds)

# ── CM 결과 판정 ────────────────────────────────────
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

# ── adset_name 파싱 ─────────────────────────────────
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

# ── Redash API 호출 ────────────────────────────────
def fetch_redash(query_id, start_date, end_date):
    import time
    url  = f'{REDASH_BASE}/api/queries/{query_id}/results?api_key={API_KEY}'
    body = {'parameters': {'start_date': start_date, 'end_date': end_date, 'group_by': 'DAY'}}
    res  = requests.post(url, json=body, timeout=120)
    data = res.json()

    if 'query_result' in data:
        return data['query_result']['data']['rows']

    if 'job' in data:
        job_id = data['job']['id']
        for _ in range(60):
            time.sleep(3)
            job_res = requests.get(f'{REDASH_BASE}/api/jobs/{job_id}?api_key={API_KEY}').json()
            if job_res['job']['status'] == 3:
                result_id = job_res['job']['query_result_id']
                r = requests.get(f'{REDASH_BASE}/api/query_results/{result_id}?api_key={API_KEY}').json()
                return r['query_result']['data']['rows']
            if job_res['job']['status'] == 4:
                raise Exception('쿼리 실패: ' + str(job_res['job'].get('error')))
        raise Exception('쿼리 타임아웃')

    raise Exception('Redash 응답 오류')

# ── 주차 레이블 변환 ────────────────────────────────
def get_week_label(date_str):
    """날짜 → 해당 주 월~일 범위 레이블 (예: 03/18~03/24)"""
    try:
        d = datetime.strptime(date_str, '%Y-%m-%d')
        monday = d - timedelta(days=d.weekday())
        sunday = monday + timedelta(days=6)
        return f"{monday.strftime('%Y-%m/%d')}~{sunday.strftime('%m/%d')}"
    except:
        return ''

# ── 주차별 시트 업데이트 (대행사용) ──────────────────
def update_weekly_sheet(gc, sheet_base, rows, ad_name_field, include_product=False):
    wb_agency = gc.open_by_key(SHEET_ID_AGENCY)

    # 주차별 집계
    weekly = {}
    for row in rows:
        adset    = row.get(ad_name_field, '')
        campaign = row.get('campaign_name', '')
        country, city, vertical = parse_adset(adset, campaign)
        cost       = row.get('cost', 0) or 0
        gmv        = row.get('gmv',  0) or 0
        con_margin = row.get('con_margin', 0) or 0
        week_label = get_week_label(row.get('basis_dt', ''))

        if include_product:
            product_id   = str(row.get('product_id_of_mall', ''))
            product_name = row.get('product_name', '')
            key = (week_label, campaign, adset, product_id, product_name)
        else:
            keyword = row.get('ad_name', '')
            key = (week_label, campaign, adset, keyword)

        if key not in weekly:
            weekly[key] = {'cost': 0, 'gmv': 0, 'con_margin': 0}
        weekly[key]['cost']       += cost
        weekly[key]['gmv']        += gmv
        weekly[key]['con_margin'] += con_margin

    # 헤더
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
                            round(cost, 0), round(v['gmv'], 0), signal])
        else:
            week_label, campaign, adset, keyword = key
            ag_data.append([week_label, campaign, adset, keyword,
                            round(cost, 0), round(v['gmv'], 0), signal])

    # 시트 탭 생성/업데이트
    tab_name    = sheet_base + '_주차별 CM 성과'
    legend_col  = 'J1' if include_product else 'I1'
    resize_cols = 11  if include_product else 10

    try:    ws = wb_agency.worksheet(tab_name)
    except: ws = wb_agency.add_worksheet(tab_name, rows=2000, cols=9)

    ws.clear()
    ws.update(ag_data, 'A1')

    legend = [['신호', '의미'], ['🟢', '성과 매우 우수 + 상향 조정 등 적극 액션 필요'],
              ['🔵', '성과 준수 + 상향 조정'], ['🟡', '성과 미달'],
              ['🔴', '성과 매우 저조 + 즉각 하향 조정 필요']]
    ws.resize(cols=resize_cols)
    ws.update(legend, legend_col)
    print(f'  {tab_name} 완료 - {len(ag_data)-1}행')

# ── 시트 업데이트 ──────────────────────────────────
def update_sheet(gc, sheet_base, rows, ad_name_field, include_product=False):
    wb        = gc.open_by_key(SHEET_ID)
    wb_agency = gc.open_by_key(SHEET_ID_AGENCY)

    cols_in = 11 if include_product else 10
    cols_ag = 8 if include_product else 7

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
        adset      = row.get(ad_name_field, '')
        campaign   = row.get('campaign_name', '')
        country, city, vertical = parse_adset(adset, campaign)
        cost       = row.get('cost', 0) or 0
        gmv        = row.get('gmv',  0) or 0
        con_margin = row.get('con_margin', 0) or 0
        cm_roas    = round(con_margin / cost * 100, 1) if cost > 0 else 0
        cm_result  = get_cm_result(cm_roas)
        signal     = get_signal(cm_roas)

        campaign = row.get('campaign_name', '')
        if include_product:
            product_id   = row.get('product_id_of_mall', '')
            product_name = row.get('product_name', '')
            in_data.append([row.get('basis_dt', ''), campaign, adset,
                            product_id, product_name, cost, gmv, con_margin, cm_roas, signal, cm_result])
            ag_data.append([row.get('basis_dt', ''), campaign, adset, product_id, product_name,
                            cost, gmv, signal])
        else:
            keyword = row.get('ad_name', '')
            in_data.append([row.get('basis_dt', ''), campaign, adset,
                            keyword, cost, gmv, con_margin, cm_roas, signal, cm_result])
            ag_data.append([row.get('basis_dt', ''), campaign, adset, keyword,
                            cost, gmv, signal])

    def safe(v):
        if v is None: return ''
        if isinstance(v, float) and (v != v): return ''  # NaN
        return v

    def write_chunks(ws, data, chunk=2000):
        import time
        safe_data = [[safe(c) for c in row] for row in data]
        ws.clear()
        for i in range(0, len(safe_data), chunk):
            for attempt in range(5):
                try:
                    ws.update(safe_data[i:i+chunk], f'A{i+1}')
                    time.sleep(1.5)
                    break
                except Exception as e:
                    if attempt == 4:
                        raise
                    print(f'    재시도 {attempt+1}/5: {e}')
                    time.sleep(10 * (attempt + 1))

    write_chunks(ws_in, in_data)
    write_chunks(ws_ag, ag_data)
    write_chunks(ws_ag2, ag_data)

    legend = [['신호', '의미'], ['🟢', '성과 매우 우수 + 상향 조정 등 적극 액션 필요'], ['🔵', '성과 준수 + 상향 조정'], ['🟡', '성과 미달'], ['🔴', '성과 매우 저조 + 즉각 하향 조정 필요']]
    legend_col  = 'J1' if include_product else 'I1'
    resize_cols = 11  if include_product else 10
    ws_ag2.resize(cols=resize_cols)
    ws_ag2.update(legend, legend_col)
    print(f'  {sheet_base} 완료 - {len(rows)}행')

# ── 메인 ───────────────────────────────────────────
def main():
    today      = datetime.today()
    start_date = (today - timedelta(days=90)).strftime('%Y-%m-%d')
    end_date   = (today - timedelta(days=1)).strftime('%Y-%m-%d')

    print('Google 인증 중...')
    gc = get_gspread_client()

    print('파워링크 데이터 가져오는 중...')
    rows_pl = fetch_redash(21679, start_date, end_date)
    update_sheet(gc, '파워링크', rows_pl, 'adset_name')
    update_weekly_sheet(gc, '파워링크', rows_pl, 'adset_name')

    print('쇼검광 데이터 가져오는 중...')
    rows_sh = fetch_redash(21686, start_date, end_date)
    update_sheet(gc, '쇼검광', rows_sh, 'adgroup_name', include_product=True)
    update_weekly_sheet(gc, '쇼검광', rows_sh, 'adgroup_name', include_product=True)

    print('전체 완료:', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

if __name__ == '__main__':
    main()
