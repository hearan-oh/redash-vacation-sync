"""
점수 시트 추가 탭 생성:
  - 파워링크/쇼검광_국가도시_주차별 점수 (현재 90일)
  - 파워링크/쇼검광_2025 7-9월 (점수)
  - 파워링크/쇼검광_6월 누적 (점수)
"""
import sys, json, os
sys.path.insert(0, '.')
from sync import (get_bq_creds, bq_query, get_score,
                  get_week_label, parse_adset, SCORE_LEGEND,
                  write_chunks, SQL_POWERLINK, SQL_SHOPPING)
import gspread
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from datetime import datetime, timedelta
from collections import defaultdict

SHEET_ID_SCORE = '1lTtCtgLRjpMV8ID4LSL7lp5s63m2GswHpYF1GMrqboE'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def get_gc():
    with open(os.path.join(BASE_DIR, 'token.json')) as f:
        t = json.load(f)
    creds = Credentials(token=t['token'], refresh_token=t['refresh_token'],
        token_uri=t['token_uri'], client_id=t['client_id'],
        client_secret=t['client_secret'], scopes=SCOPES)
    if creds.expired:
        creds.refresh(Request())
    return gspread.authorize(creds)


def get_or_create(wb, name, rows=5000, cols=10):
    try:    return wb.worksheet(name)
    except: return wb.add_worksheet(name, rows=rows, cols=cols)


def write_tab(wb, name, data, legend_col, resize_cols):
    ws = get_or_create(wb, name)
    write_chunks(ws, data)
    ws.resize(cols=resize_cols)
    print(f'  [{name}] 완료 - {len(data)-1}행')


def build_city_weekly(rows):
    grouped = defaultdict(lambda: {'cost': 0, 'gmv': 0, 'cm': 0})
    for row in rows:
        adset    = row.get('adset_name') or row.get('adgroup_name', '') or ''
        campaign = row.get('campaign_name', '') or ''
        cost     = float(row.get('cost', 0) or 0)
        gmv      = float(row.get('gmv', 0) or 0)
        cm       = float(row.get('con_margin', 0) or 0)
        week     = get_week_label(str(row.get('basis_dt', ''))[:10])
        country, city, _ = parse_adset(adset, campaign)
        if not country:
            continue
        k = (week, country, city)
        grouped[k]['cost'] += cost
        grouped[k]['gmv']  += gmv
        grouped[k]['cm']   += cm

    data = [['주차', '국가', '도시', '광고비', 'GMV', '점수']]
    for k in sorted(grouped.keys(), reverse=True):
        v    = grouped[k]
        cost = v['cost']
        cm_roas = round(v['cm'] / cost * 100, 1) if cost > 0 else 0
        data.append([k[0], k[1], k[2], round(cost), round(v['gmv']), get_score(cm_roas)])
    return data


def build_monthly(rows, is_shopping=False):
    grouped = defaultdict(lambda: {'cost': 0, 'gmv': 0, 'cm': 0})
    for row in rows:
        campaign = row.get('campaign_name', '') or ''
        adset    = row.get('adset_name', '') or ''
        cost     = float(row.get('cost', 0) or 0)
        gmv      = float(row.get('gmv', 0) or 0)
        cm       = float(row.get('con_margin', 0) or 0)
        month    = str(row.get('basis_dt', ''))[:7]
        if is_shopping:
            pid   = row.get('product_id_of_mall', '') or ''
            pname = row.get('product_name', '') or ''
            k = (month, campaign, adset, pid, pname)
        else:
            keyword = row.get('ad_name', '') or ''
            k = (month, campaign, adset, keyword)
        grouped[k]['cost'] += cost
        grouped[k]['gmv']  += gmv
        grouped[k]['cm']   += cm

    if is_shopping:
        headers = ['월', '캠페인명', '그룹명', '상품ID', '상품명', '광고비', 'GMV', '점수']
    else:
        headers = ['월', '캠페인명', '그룹명', '키워드', '광고비', 'GMV', '점수']

    data = [headers]
    for k in sorted(grouped.keys(), reverse=True):
        v    = grouped[k]
        cost = v['cost']
        cm_roas = round(v['cm'] / cost * 100, 1) if cost > 0 else 0
        score   = get_score(cm_roas)
        if is_shopping:
            data.append([k[0], k[1], k[2], k[3], k[4], round(cost), round(v['gmv']), score])
        else:
            data.append([k[0], k[1], k[2], k[3], round(cost), round(v['gmv']), score])
    return data


def main():
    bq_creds = get_bq_creds()
    gc = get_gc()
    wb = gc.open_by_key(SHEET_ID_SCORE)

    today      = datetime.today()
    start_90d  = (today - timedelta(days=90)).strftime('%Y-%m-%d')
    end_yest   = (today - timedelta(days=1)).strftime('%Y-%m-%d')

    # ── 현재 90일 데이터 (국가도시_주차별 + 6월 누적) ──
    print(f'파워링크 조회 중... ({start_90d} ~ {end_yest})')
    rows_pl = bq_query(bq_creds, SQL_POWERLINK.format(start_date=start_90d, end_date=end_yest))
    print(f'  {len(rows_pl)}행')

    print(f'쇼검광 조회 중...')
    rows_sh = bq_query(bq_creds, SQL_SHOPPING.format(start_date=start_90d, end_date=end_yest))
    print(f'  {len(rows_sh)}행')

    # ── 2025 7-9월 데이터 ──
    print('2025 7-9월 파워링크 조회 중...')
    rows_pl_2025 = bq_query(bq_creds, SQL_POWERLINK.format(start_date='2025-07-01', end_date='2025-09-30'))
    print(f'  {len(rows_pl_2025)}행')

    print('2025 7-9월 쇼검광 조회 중...')
    rows_sh_2025 = bq_query(bq_creds, SQL_SHOPPING.format(start_date='2025-07-01', end_date='2025-09-30'))
    print(f'  {len(rows_sh_2025)}행')

    print('시트 업데이트 중...')

    # 국가도시_주차별 점수 (주차/국가/도시/광고비/GMV/점수 = 6컬럼)
    write_tab(wb, '파워링크_국가도시_주차별 점수', build_city_weekly(rows_pl), 'G1', 8)
    write_tab(wb, '쇼검광_국가도시_주차별 점수',   build_city_weekly(rows_sh),  'G1', 8)

    # 6월 누적
    rows_pl_jun = [r for r in rows_pl if str(r.get('basis_dt',''))[:7] == '2026-06']
    rows_sh_jun = [r for r in rows_sh if str(r.get('basis_dt',''))[:7] == '2026-06']
    write_tab(wb, '파워링크_6월 누적', build_monthly(rows_pl_jun),              'H1', 9)
    write_tab(wb, '쇼검광_6월 누적',   build_monthly(rows_sh_jun, is_shopping=True), 'I1', 10)

    # 2025 7-9월
    write_tab(wb, '파워링크_2025 7-9월', build_monthly(rows_pl_2025),              'H1', 9)
    write_tab(wb, '쇼검광_2025 7-9월',   build_monthly(rows_sh_2025, is_shopping=True), 'I1', 10)

    print(f'전체 완료: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')


if __name__ == '__main__':
    main()
