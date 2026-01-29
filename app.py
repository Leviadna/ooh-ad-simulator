# pip install streamlit pandas numpy plotly pydeck google-cloud-bigquery google-auth

import streamlit as st
import pandas as pd
import numpy as np
import streamlit.components.v1 as components
import json
import plotly.express as px
import pydeck as pdk
from google.cloud import bigquery
from google.oauth2 import service_account
from concurrent.futures import ThreadPoolExecutor

# 카카오맵 API
KAKAO_API_KEY = st.secrets['KAKAO_API_KEY']

# 페이지 설정
st.set_page_config(
    page_title='옥외광고 효과 분석 시뮬레이터',
    page_icon='📊',
    layout='wide'
)

# 화면 최소 너비 고정
def enforce_min_width():
    st.markdown("""<style>div.block-container{min-width:1280px}</style>""", unsafe_allow_html=True)

enforce_min_width()

# -----------------------------------------------------------
# 1. 빅쿼리 연결 설정
# -----------------------------------------------------------
PROJECT_ID = 'data-485606'
DATASET_ID = 'postgresql'

# 1.1. 빅쿼리 연결
@st.cache_resource
def get_bq_client():
    
    try:
        key_dict = dict(st.secrets['gcp_service_account'])

        credentials = service_account.Credentials.from_service_account_info(key_dict)
        client = bigquery.Client(credentials=credentials, project=key_dict['project_id'])
        return client
        
    except Exception as e:
        st.error(f"BigQuery 연결 실패: {e}")
        return None

# 1.2. 데이터 로드
@st.cache_data(ttl=600)
def load_data():
    client = get_bq_client()
    if client is None:
        return None, None, None, None, None, None

    try:
        # 테이블 목록 정의 및 변수 할당
        tables = {
            'digital': f"{PROJECT_ID}.{DATASET_ID}.digital",
            'factor': f"{PROJECT_ID}.{DATASET_ID}.factor_prediction_result",
            'kpi': f"{PROJECT_ID}.{DATASET_ID}.kpi",
            'package': f"{PROJECT_ID}.{DATASET_ID}.package",
            'shelter': f"{PROJECT_ID}.{DATASET_ID}.shelter",
            'demographics': f"{PROJECT_ID}.{DATASET_ID}.demographics"
        }

        data = {}

        def fetch_table(key, table_id):
            return key, client.list_rows(table_id).to_dataframe()
            
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(fetch_table, k, tid) for k, tid in tables.items()]

            for future in futures:
                key, df = future.result()
                data[key] = df

        digital = data['digital']
        factor = data['factor']
        kpi = data['kpi']
        package = data['package']
        shelter = data['shelter']
        demographics = data['demographics']
            
        # 전처리
        for df in [digital, kpi, package, shelter, demographics]:
            if 'ftr_idn' in df.columns:
                df['ftr_idn'] = df['ftr_idn'].astype(str)
            if 'month' in df.columns:
                df['month'] = df['month'].astype(str)

        cols_to_numeric = ['stay_time', 'share_of_time']
        for col in cols_to_numeric:
            if col in digital.columns:
                digital[col] = pd.to_numeric(digital[col], errors='coerce')

        for col in ['rots', 'reach']:
            if col in kpi.columns:
                kpi[col] = pd.to_numeric(kpi[col], errors='coerce').fillna(0)
            if col in demographics.columns:
                demographics[col] = pd.to_numeric(demographics[col], errors='coerce').fillna(0)

        str_cols = ['shelter_type', 'media_type']
        for col in str_cols:
            if col in kpi.columns:
                kpi[col] = kpi[col].astype(str).str.strip()

        return digital, factor, kpi, package, shelter, demographics

    except Exception as e:
        st.error(f"데이터 로드 중 오류 발생: {e}")
        return None, None, None, None, None, None

digital, factor_df, kpi, package, shelter_info, demographics = load_data()

def save_package_to_bq(pkg_name, pkg_type, id_list):
    client = get_bq_client()
    if client is None:
        return False

    try:
        new_data = pd.DataFrame({
            'package_name': [pkg_name] * len(id_list),
            'package_type': [pkg_type] * len(id_list),
            'ftr_idn': id_list
        })

        table_id = f"{PROJECT_ID}.{DATASET_ID}.package"

        job_config = bigquery.LoadJobConfig(write_disposition='WRITE_APPEND')
        job = client.load_table_from_dataframe(new_data, table_id, job_config=job_config)
        job.result()

        return True
    except Exception as e:
        st.error(f"DB 저장 실패: {e}")
        return False

# -----------------------------------------------------------
# 2. 유틸리티 함수
# -----------------------------------------------------------
def get_color_by_type(shelter_type):
    colors = {
        '가로변 쉘터': '#153b5d',       
        '중앙차로버스 쉘터': '#00b8bc', 
        '환승센터': '#ffc000',          
        '관광안내판': '#fc766a',        
        '마을버스 쉘터': '#3247a6',     
    }
    return colors.get(shelter_type, '#808080')

def render_kakao_map(lat, lon, zoom_level, data):
    map_data_json = json.dumps(data)
    html_code = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset='utf-8'>
        <meta http-equiv="Content-Security-Policy" content="upgrade-insecure-requests">
        <style>#map{{width:100%;height:600px;border-radius:10px}}</style>
    </head>
    <body>
        <div id='map'></div>
        <script type='text/javascript' src='https://dapi.kakao.com/v2/maps/sdk.js?appkey={KAKAO_API_KEY}'></script>
        <script>
            var container = document.getElementById('map');
            var options = {{ center: new kakao.maps.LatLng({lat}, {lon}), level: {zoom_level} }};
            var map = new kakao.maps.Map(container, options);
            var positions = {map_data_json};
            var circles = [];

            function getRadiusByLevel(level) {{
                if (level <= 5) {{
                    return 50;
                }} else if (level <= 8) {{
                    return 150;
                }} else {{
                    return 300;
                }}
            }}
            
            positions.forEach(function(pos) {{
                var initRadius = getRadiusByLevel(map.getLevel());
                var circle = new kakao.maps.Circle({{
                    center : new kakao.maps.LatLng(pos.lat, pos.lng),
                    radius: initRadius, strokeWeight: 2, strokeColor: '#ffffff', strokeOpacity: 0.9,
                    strokeStyle: 'solid', fillColor: pos.color, fillOpacity: 0.8 
                }}); 
                circle.setMap(map);
                circles.push(circle);
                
                var iwContent = '<div style="padding:5px; font-size:12px; color:#000;">' + \
                                '<b>' + pos.name + '</b><br>' + 'ROTS: ' + pos.rots + '<br>' + 'Reach: ' + pos.reach + '</div>';
                var infowindow = new kakao.maps.InfoWindow({{ content : iwContent }});
                
                kakao.maps.event.addListener(circle, 'mouseover', function() {{
                    infowindow.setPosition(circle.getPosition());
                    infowindow.open(map);
                }});
                kakao.maps.event.addListener(circle, 'mouseout', function() {{
                    infowindow.close();
                }});
            }});

            kakao.maps.event.addListener(map, 'zoom_changed', function() {{
                var level = map.getLevel();
                var newRadius = getRadiusByLevel(level);

                for (var i = 0; i < circles.length; i++) {{
                    circles[i].setOptions({{radius: newRadius}});
                }}
            }});
            
            var zoomControl = new kakao.maps.ZoomControl();
            map.addControl(zoomControl, kakao.maps.ControlPosition.RIGHT);
        </script>
    </body>
    </html>
    """
    components.html(html_code, height=600)

# -----------------------------------------------------------
# 3. 사이드바 UI
# -----------------------------------------------------------
with st.sidebar:
    st.title('검색 속성')
    st.markdown("""
        <style>
            div[data-testid="stMarkdownContainer"] hr{margin:8px 0 20px!important}
            div[data-testid="stElementContainer"]{width:100%}
            div[data-baseweb="button-group"]{display:flex;justify-content:center;max-width:100%;column-gap:1rem}
            div[data-baseweb="button-group"] button{padding:0.5rem 1.5rem}
            div[data-testid="InputInstructions"]{display:none}
        </style>
    """, unsafe_allow_html=True)
    
    filter_mode = st.pills(
        '분석 유형',
        ['패키지', '관심 매체'],
        selection_mode='single',
        default='패키지',
        label_visibility='collapsed'
    )
    if not filter_mode: filter_mode = '패키지'
        
    # st.markdown('---')
    final_selected_idns = []
    
    # 3.1. 패키지 선택
    if filter_mode == '패키지':
        # 3.1.1. 상위 필터
        with st.expander('기본 필터링', expanded=True):
            
            if kpi is not None:
                available_months = sorted(kpi['month'].unique(), reverse=True)
                if len(available_months) > 0:
                    selected_month = st.selectbox('기간', available_months)
                else:
                    st.warning('데이터 부족')
                    selected_month = None
            
            if package is not None and selected_month is not None:
                raw_pkg_list = sorted(package['package_name'].unique())
                option_list = ['전체 (디지털)', '전체 (포스터)'] + raw_pkg_list
                
                selected_package_option = st.selectbox('패키지', option_list, index=0)
                
                # 패키지에 따른 ID 1차 필터링
                pkg_shelters = []
                
                if '전체 (' in selected_package_option:
                    is_view_all = True
                    if kpi is not None:
                        pkg_shelters = kpi[kpi['month'] == selected_month]['ftr_idn'].unique()
                        
                        pkg_mapping = pd.DataFrame({'ftr_idn': pkg_shelters})
                        
                        if '디지털' in selected_package_option:
                            pkg_mapping['package_type'] = 'D'
                        else:
                            pkg_mapping['package_type'] = 'P'
                else:
                    is_view_all = False
                    pkg_filtered = package[package['package_name'] == selected_package_option].copy()
                    pkg_mapping = pkg_filtered[['ftr_idn', 'package_type']].drop_duplicates('ftr_idn')
                    pkg_shelters = pkg_mapping['ftr_idn'].unique()
                
                if len(pkg_shelters) > 0:
                    current_context_df = kpi[
                        (kpi['month'] == selected_month) & 
                        (kpi['ftr_idn'].isin(pkg_shelters))
                    ]
                    
                    # --- 설치 유형 ---
                    avail_shelter_types = sorted(current_context_df['shelter_type'].unique())
                    shelter_options = ['전체'] + avail_shelter_types
                    selected_shelter_type = st.selectbox('설치 유형', shelter_options)
                    
                    # --- 매체 유형 ---
                    avail_media_types = sorted(current_context_df['media_type'].unique())
                    media_options = ['전체'] + avail_media_types
                    selected_media_type = st.selectbox('매체 유형', media_options)

                    # 필터 적용
                    if selected_shelter_type != '전체':
                        current_context_df = current_context_df[current_context_df['shelter_type'] == selected_shelter_type]
                    
                    if selected_media_type != '전체':
                        current_context_df = current_context_df[current_context_df['media_type'] == selected_media_type]
                    
                    pkg_shelters = current_context_df['ftr_idn'].unique()
                    
            else:
                pkg_shelters = []
                                
        # 3.1.2. 검색창
        search_keyword = st.text_input('검색', placeholder='매체명을 입력하세요.')
        
        if search_keyword and len(pkg_shelters) > 0 and kpi is not None:
            search_result = kpi[
                (kpi['month'] == selected_month) &
                (kpi['ftr_idn'].isin(pkg_shelters)) &
                (kpi['shelter_name'].str.contains(search_keyword, case=False, na=False))
            ]
            # 검색 결과로 ID 리스트 갱신
            pkg_shelters = search_result['ftr_idn'].unique()
    
        final_selected_idns = pkg_shelters

    # 3.2. 매체 직접 선택
    else:
        with st.expander('ID 입력 및 확인', expanded=True):

            # 3.2.1. 패키지 속성 선택
            input_pkg_type = st.selectbox('패키지 유형', ['디지털', '포스터'])
            real_pkg_type = 'D' if input_pkg_type == '디지털' else 'P'
            
            # 3.2.2. 기간 선택
            if kpi is not None:
                available_months = sorted(kpi['month'].unique(), reverse=True)
                selected_month = st.selectbox('분석 기간 설정', available_months, key='custom_month_select')
            else:
                selected_month = None
                
            # 3.2.3. 텍스트 영역
            input_text = st.text_area(
                'ID 입력',
                placeholder='ex. 10567,10445,10334',
                height=150
            )

            # 3.2.4. 유효성 검사 및 데이터 매핑
            if input_text and selected_month and kpi is not None:
                raw_ids = [x.strip() for x in input_text.replace('\n', ',').split(',') if x.strip()]
                valid_shelters = kpi[(kpi['month'] == selected_month) & (kpi['ftr_idn'].isin(raw_ids))]
                found_ids = valid_shelters['ftr_idn'].unique()

                if len(found_ids) > 0:
                    st.success(f"총 {len(raw_ids)}개 중 {len(found_ids)}개 매체 확인")

                    temp_mapping = pd.DataFrame({'ftr_idn': found_ids})
                    temp_mapping['package_type'] = real_pkg_type
                    pkg_mapping = temp_mapping

                    final_selected_idns = found_ids

                    # 3.2.5 패키지 저장
                    with st.form('save_pkg_form'): 
                        new_pkg_name = st.text_input('패키지 저장', placeholder='ex. 뷰티')
                        save_submitted = st.form_submit_button('DB 저장', use_container_width=True)

                        if save_submitted:
                            if new_pkg_name:
                                if package is not None and new_pkg_name in package['package_name'].unique():
                                    st.error('이미 존재하는 패키지명입니다.')
                                else: 
                                    success = save_package_to_bq(new_pkg_name, real_pkg_type, found_ids)
                                    if success:
                                        st.success(f"'{new_pkg_name}' 패키지가 저장되었습니다.")
                                        st.cache_data.clear()
                                        st.rerun()
                            else:
                                st.warning('패키지명을 입력해주세요.')
                else:
                    st.error('분석 매체 중 유효하지 않은 매체가 존재합니다.')
                    final_selected_idns = []
            else:
                final_selected_idns = []

    # 3.3. 성연령별 필터링
    selected_gender = '전체'
    selected_age_code = 0

    with st.expander('성연령 필터링'):
        if demographics is not None:
            c_gen, c_age = st.columns(2)
            with c_gen:
                gender_map = {'전체': '전체', '남성': 'M', '여성': 'F'}
                selected_gender_label = st.selectbox('성별', list(gender_map.keys()))
                selected_gender = gender_map[selected_gender_label]
            with c_age: 
                age_map_inv = {'전체': 0, '10대 이하': 1, '20대': 2, '30대': 3, '40대': 4, '50대': 5, '60대': 6, '70대 이상': 7}
                selected_age_label = st.selectbox('연령대', list(age_map_inv.keys()))
                selected_age_code = age_map_inv[selected_age_label]

    # 3.4. 원본 데이터 보기
    st.markdown('---')
    calc_mode = st.selectbox('산출 옵션', ['기본', '디지털 공식 미적용', '디지털 풀 구좌'], index=0, help='분석할 시뮬레이션 시나리오를 선택합니다.')

# -----------------------------------------------------------
# 4. 메인 산출 로직
# -----------------------------------------------------------
if kpi is not None and len(final_selected_idns) > 0:
    
    # 4.1. 데이터 병합
    target_kpi = kpi[(kpi['month'] == selected_month) & (kpi['ftr_idn'].isin(final_selected_idns))].copy()
    is_demo_filtered = (selected_gender != '전체') or (selected_age_code != 0)

    if is_demo_filtered and demographics is not None:
        # 조건에 부합하는 성연령 데이터 필터링
        demo_subset_mask = (demographics['month'] == selected_month) & (demographics['ftr_idn'].isin(final_selected_idns))

        if selected_gender != '전체':
            demo_subset_mask &= (demographics['gender'] == selected_gender)
        if selected_age_code != 0:
            demo_subset_mask &= (demographics['age'] == selected_age_code)

        demo_subset = demographics[demo_subset_mask].copy()
        # ID별 그룹화하여 ROTS, Reach 산출
        grouped_demo = demo_subset.groupby('ftr_idn')[['rots', 'reach']].sum().reset_index()
        # 기존 KPI 테이블에서 총합 제거 후 필터링된 합계로 병합
        target_kpi = target_kpi.drop(columns=['rots', 'reach'])
        target_kpi = pd.merge(target_kpi, grouped_demo, on='ftr_idn', how='left', suffixes=('', '_digital'))
        target_kpi[['rots', 'reach']] = target_kpi[['rots', 'reach']].fillna(0)
    
    target_kpi = pd.merge(target_kpi, pkg_mapping, on='ftr_idn', how='left')
    merged = pd.merge(target_kpi, digital, on=['month', 'ftr_idn'], how='left', suffixes=('', '_digital'))
    merged = pd.merge(merged, shelter_info[['ftr_idn', 'longitude', 'latitude', 'grade']], on='ftr_idn', how='left')

    # 4.2. ROTS 및 Reach 계산
    def calculate_metrics_row(row):
        k_rots = row['rots']
        k_reach = row['reach']
        if calc_mode == '디지털 공식 미적용':
            return pd.Series([k_rots, k_reach], index=['adj_rots', 'adj_reach'])
                
        pkg_type = row.get('package_type', 'P')
        s_type = row['shelter_type']
        m_type = row['media_type']
        
        # Case A: 관광안내판 & 포스터
        if (s_type == '관광안내판') and (m_type == '포스터'):
            return pd.Series([k_rots / 2.0, k_reach / 2.0], index=['adj_rots', 'adj_reach'])

        # Case B: 디지털 공식 적용 판단
        apply_digital_formula = (pkg_type == 'D') or (m_type == '디지털')
        
        if apply_digital_formula:
            if pd.notnull(row['stay_time']):
                stay_time = float(row['stay_time'])
                
                if calc_mode == '디지털 풀 구좌':
                    share_of_time = 0.05
                else:
                    share_of_time = float(row['share_of_time'])
                time_factor = (max(stay_time - 1, 0) + 30) / 30.0
                adj_rots = time_factor * k_rots * share_of_time * 0.5
                adj_reach = time_factor * k_reach * share_of_time * 0.5
                
                return pd.Series([adj_rots, adj_reach], index=['adj_rots', 'adj_reach'])
            else:
                return pd.Series([k_rots, k_reach], index=['adj_rots', 'adj_reach'])
        
        # Case C: 일반
        return pd.Series([k_rots, k_reach], index=['adj_rots', 'adj_reach'])

    metrics_df = merged.apply(calculate_metrics_row, axis=1)
    merged['adj_rots'] = metrics_df['adj_rots']
    merged['adj_reach'] = metrics_df['adj_reach']
    
    # 4.3. 총합 및 가중치 적용
    total_shelters = len(merged)
    sum_adj_rots = merged['adj_rots'].sum()
    sum_adj_reach = merged['adj_reach'].sum()

    region_pkg_weights = {'강남D': 0.5430, '서초D': 0.7165, '이태원D': 0.3311, '종로D': 0.4892, '종로중구MD': 0.5442}
    
    correction_val = 0

    if total_shelters > 0:
        if filter_mode == '패키지' and selected_package_option in region_pkg_weights:
            correction_val = region_pkg_weights[selected_package_option]
        else:
            if factor_df is not None:
                max_qty = factor_df['quantity'].max()
                lookup_qty = min(total_shelters, max_qty)

                found_val = factor_df.loc[factor_df['quantity'] == lookup_qty, 'correction_factor']
                if not found_val.empty:
                    correction_val = found_val.values[0]
                else:
                    correction_val = 0
            else:
                correction_val = 0
    else:
        correction_val = 0

    final_total_reach = sum_adj_reach * correction_val
    
    # 4.4. 결과 시각화
    if filter_mode == '패키지':
        title_prefix = '전체' if is_view_all else f"패키지 [{selected_package_option}]"
    else: 
        title_prefix = '관심 매체'
        
    st.title(f"📊 {selected_month} {title_prefix} 광고 효과 분석")
    
    metrics_placeholder = st.empty()
    st.markdown("---")
    
    tab1, tab2 = st.tabs(['메인 대시보드', '성연령별 분석'])

    # 4.4.1. Tab 1: 메인 대시보드
    with tab1:
        c1, c2 = st.columns([2, 1])

        with c2:
            st.subheader('매체별 데이터')
            filter_c1, filter_c2 = st.columns(2)

            with filter_c1:
                sort_col_ui = st.selectbox('정렬 기준', ['ROTS', 'Reach'])

            with filter_c2:
                max_len = len(merged)
                if max_len > 0:
                    top_n = st.number_input('상위 N개 조회', min_value=1, max_value=max_len, value=max_len, step=10)
                else:
                    top_n = 0
                    st.number_input('상위 N개 조회', disabled=True, value=0)

            sort_target = 'adj_rots' if sort_col_ui == 'ROTS' else 'adj_reach'
            final_df = merged.sort_values(sort_target, ascending=False).reset_index(drop=True)

            if top_n > 0:
                final_df = final_df.head(top_n)

            cur_count = len(final_df)
            cur_rots = final_df['adj_rots'].sum()
            cur_reach = final_df['adj_reach'].sum()
            region_pkg_weights_map = {'강남D': 0.5430, '서초D': 0.7165, '이태원D': 0.3311, '종로D': 0.4892, '종로중구MD': 0.5442}
            cur_correction = 0

            if cur_count > 0:
                if filter_mode == '패키지' and selected_package_option in region_pkg_weights_map:
                    cur_correction = region_pkg_weights_map[selected_package_option]
                else:
                    if factor_df is not None:
                        max_qty = factor_df['quantity'].max()
                        lookup_qty = min(cur_count, max_qty) # 잘린 개수 기준
                        found_val = factor_df.loc[factor_df['quantity'] == lookup_qty, 'correction_factor']
                        cur_correction = found_val.values[0] if not found_val.empty else 0
                    else:
                        cur_correction = 0
            
            final_cur_reach = cur_reach * cur_correction

            d_df = final_df[['shelter_name', 'shelter_type', 'media_type', 'adj_rots', 'adj_reach']].copy()
            d_df.columns = ['매체명', '설치 유형', '매체 유형', 'ROTS', 'Reach']
            d_df.index += 1
            st.dataframe(d_df.style.format({'ROTS': '{:,.0f}', 'Reach': '{:,.0f}'}), height=600, use_container_width=True)

            with metrics_placeholder.container():
                col1, col2, col3, col4, col5 = st.columns(5)
                col1.metric('매체 수', f"{cur_count:,}")
                col2.metric('총 ROTS', f"{cur_rots:,.0f}")
                col3.metric('총 Reach', f"{final_cur_reach:,.0f}")
                freq = (cur_rots / final_cur_reach) if final_cur_reach > 0 else 0
                col4.metric('Frequency', f"{freq:,.2f}")
                col5.metric('가중치', f"{cur_correction:.4f}")
                
        with c1:
            map_header_col, map_select_col = st.columns([4, 1])
            
            with map_header_col:
                st.subheader("매체 지도")
            with map_select_col:
                map_type = st.radio('지도 타입 선택', ['Kakao', 'Dark'], horizontal=True,label_visibility='collapsed')

            map_df = final_df[['latitude', 'longitude', 'shelter_name', 'adj_rots', 'adj_reach', 'shelter_type']].dropna(subset=['latitude', 'longitude'])
            map_df['color'] = map_df['shelter_type'].apply(get_color_by_type)
            
            if map_type == 'Kakao':
                k_data = [{
                    'lat': r['latitude'], 
                    'lng': r['longitude'], 
                    'name': r['shelter_name'], 
                    'color': r['color'],
                    'rots': f"{r['adj_rots']:,.0f}",
                    'reach': f"{r['adj_reach']:,.0f}"
                } for _, r in map_df.iterrows()]

                clat = map_df['latitude'].mean() if not map_df.empty else 37.5665
                clon = map_df['longitude'].mean() if not map_df.empty else 126.9780
                czoom = 7 if not map_df.empty else 9
                
                if not KAKAO_API_KEY: st.warning('API 키 없음')
                else: render_kakao_map(clat, clon, czoom, k_data)
            else:
                st.map(map_df, latitude='latitude', longitude='longitude', color='color', zoom=11, use_container_width=True, height=600)
            legend_html = """
            <div style='
                background-color:rgba(20,20,20,0.7);
                padding:10px 15px; 
                border-radius:8px; 
                color:#ffffff;
                font-size:13px;
                margin-top:10px;
                border: 1px solid rgba(255,255,255,0.1)
            '>
                <div style='display:flex;flex-direction:row;gap:15px;flex-wrap:wrap;align-items:center'>
                    <span><span style='color:#153b5d;font-size:16px;'>●</span> 가로변 쉘터</span>
                    <span><span style='color:#00b8bc;font-size:16px;'>●</span> 중앙차로버스 쉘터</span>
                    <span><span style='color:#ffc000;font-size:16px;'>●</span> 환승센터</span>
                    <span><span style='color:#fc766a;font-size:16px;'>●</span> 관광안내판</span>
                    <span><span style='color:#3247a6;font-size:16px;'>●</span> 마을버스 쉘터</span>
                </div>
            </div>
            """
            st.markdown(legend_html, unsafe_allow_html=True)

    # 4.4.2. Tab 2: 성연령별 분석
    with tab2:
        st.subheader('성연령별 분석')
        target_ids = final_df['ftr_idn'].unique()
        
        if demographics is not None and not demographics.empty:
            demo_mask = (demographics['month'] == selected_month) & (demographics['ftr_idn'].isin(final_selected_idns))

            if selected_gender != '전체':
                demo_mask &= (demographics['gender'] == selected_gender)
            if selected_age_code != 0:
                demo_mask &= (demographics['age'] == selected_age_code)

            target_demo = demographics[demo_mask].copy()
            
            if not target_demo.empty:
                # final_df 기준 Ratio 매핑
                final_df['calc_ratio'] = np.where(final_df['reach'] > 0, final_df['adj_reach'] / final_df['reach'], 1.0)
                ratio_map = final_df[['ftr_idn', 'calc_ratio']].set_index('ftr_idn')
                
                target_demo = target_demo.join(ratio_map, on='ftr_idn')
                target_demo['calc_ratio'] = target_demo['calc_ratio'].fillna(1.0)
                
                target_demo['adj_demo_rots'] = target_demo['rots'] * target_demo['calc_ratio']
                target_demo['adj_demo_reach'] = target_demo['reach'] * target_demo['calc_ratio']
                
                gender_summ = target_demo.groupby('gender')[['adj_demo_rots', 'adj_demo_reach']].sum().reset_index()
                age_summ = target_demo.groupby('age')[['adj_demo_rots', 'adj_demo_reach']].sum().reset_index()
                
                # 가중치 적용
                gender_summ['adj_demo_reach'] = gender_summ['adj_demo_reach'] * cur_correction
                age_summ['adj_demo_reach'] = age_summ['adj_demo_reach'] * cur_correction
                
                gc1, gc2 = st.columns(2)
                age_map_disp = {1: '10대 이하', 2: '20대', 3: '30대', 4: '40대', 5: '50대', 6: '60대', 7: '70대 이상'}
                gender_map_disp = {'M': '남성', 'F': '여성'}
                
                with gc1:
                    st.markdown('성별 비중')
                    gender_summ['gender'] = gender_summ['gender'].map(gender_map_disp)
                    fig_gender = px.pie(gender_summ, values='adj_demo_reach', names='gender', 
                                        color='gender', color_discrete_map={'남성':'#36a2eb', '여성':'#ff6384'},
                                        hole=0.4)
                    fig_gender.update_traces(
                        texttemplate='%{value:,.0f}<br>%{percent:.2%}', 
                        hovertemplate='성별: %{label}<br>수치: %{value:,.0f}<br>비율: %{percent:.2%}<extra></extra>'
                    )
                    fig_gender.update_layout(hoverlabel=dict(namelength=-1), hovermode='closest')
                    st.plotly_chart(fig_gender, use_container_width=True)
                    
                with gc2:
                    st.markdown('연령대별 분포')
                    age_summ['age_label'] = age_summ['age'].map(age_map_disp)
                    fig_age = px.bar(age_summ, x='age_label', y='adj_demo_reach', 
                                     labels={'age_label': '연령대', 'adj_demo_reach': 'Reach'},
                                     color_discrete_sequence=['#4bc0c0'],
                                     text_auto=',.0f')
                    fig_age.update_traces(
                        texttemplate='%{y:,.0f}',
                        textposition='outside',
                        hovertemplate='연령대: %{x}<br>Reach: %{y:,.0f}<extra></extra>'
                    )
                    fig_age.update_layout(yaxis_tickformat=',', xaxis_title=None)
                    st.plotly_chart(fig_age, use_container_width=True)
                
                with st.expander('성연령별 상세 데이터 보기'):
                    pivot_demo = target_demo.groupby(['age', 'gender'])[['adj_demo_rots', 'adj_demo_reach']].sum().reset_index()
                    pivot_demo['adj_demo_reach'] = pivot_demo['adj_demo_reach'] * cur_correction

                    pivot_demo['age'] = pivot_demo['age'].map(age_map_disp)
                    pivot_demo['gender'] = pivot_demo['gender'].map(gender_map_disp)
                    
                    pivot_demo = pivot_demo.rename(columns={'age':'연령', 'gender':'성별', 'adj_demo_rots':'ROTS', 'adj_demo_reach':'Reach'})
                    st.dataframe(pivot_demo.style.format({'ROTS': '{:,.0f}', 'Reach': '{:,.0f}'}), use_container_width=True)
            else:
                st.info('선택된 매체에 해당하는 성연령 데이터가 없습니다.')
        else:
            st.warning('demographics 테이블을 불러오지 못했습니다.')

elif kpi is None:
    st.warning('데이터 로드중입니다. 연결 실패 시 BigQuery 조회가 필요합니다.')
else:
    if filter_mode == '패키지':
        st.info('좌측 사이드바에서 패키지를 선택해주세요.')
    else:
        st.info('좌측 사이드바에서 매체를 추가해주세요.')
