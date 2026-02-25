import json
import os
import glob
import re

import pandas as pd

from translate.utils.loader import BronzeDataName, get_dataset
from translate.utils.paths import METADATA_ARTIFACT_DIR, BRONZE_DIR

def save_bronze_relationship(save_file_name):
    df_dict: dict[BronzeDataName, pd.DataFrame] = {}
    for member in BronzeDataName:
        df_dict[member], _ = get_dataset(member)
        df_dict[member] = df_dict[member].drop_duplicates()

    columns_by_df = {}
    for enum_name, df in df_dict.items():
        columns = sorted(df.columns.to_list())
        columns_by_df[enum_name] = columns

    all_unique_columns = []
    for columns in list(columns_by_df.values()):
        all_unique_columns += columns
    all_unique_columns = sorted(set(all_unique_columns))

    unique_columns_by_df = {}
    for unique_col in all_unique_columns:
        unique_columns_by_df[unique_col] = []

    for enum_name, cols in columns_by_df.items():
        for unique_col in unique_columns_by_df.keys():
            if unique_col in cols:
                unique_columns_by_df[unique_col].append(enum_name.value)
                
    columns_shared_across_bronze = dict(filter(lambda item: len(item[1]) > 1, unique_columns_by_df.items()))
    save_path = os.path.join(METADATA_ARTIFACT_DIR, save_file_name)
    with open(save_path, 'w', encoding='utf-8') as f:
        json.dump(columns_shared_across_bronze, f, indent=4, ensure_ascii=False)

def save_bronze_column_description(save_file_name):
    column_descriptions = \
"""enum,column_name,description
customers,customer_id,고객을 식별하기 위한 고유 ID
customers,customer_unique_id,고객의 고유 식별자(중복되지 않는 ID)
customers,customer_zip_code_prefix,고객의 우편번호 접두사
customers,customer_city,고객이 거주하는 도시
customers,customer_state,고객이 거주하는 주(state)
geolocation,geolocation_zip_code_prefix,위치 정보에 해당하는 우편번호 접두사
geolocation,geolocation_lat,위도(latitude)
geolocation,geolocation_lng,경도(longitude)
geolocation,geolocation_city,위치 정보에 해당하는 도시
geolocation,geolocation_state,위치 정보에 해당하는 주(state)
order_items,order_id,주문을 식별하기 위한 고유 ID
order_items,order_item_id, 해당 주문 내에서 각 상품 아이템을 구분하는 번호
order_items,product_id,상품을 식별하기 위한 고유 ID
order_items,seller_id,판매자를 식별하기 위한 고유 ID
order_items,shipping_limit_date,판매자가 상품을 배송사에 전달해야 하는 기한
order_items,price,상품 가격
order_items,freight_value,배송비(운송 비용)
order_payments,order_id,주문을 식별하기 위한 고유 ID
order_payments,payment_sequential,결제 순서(결제와 관련된 순차적 번호)
order_payments,payment_type,결제 유형(예: credit_card, boleto 등)
order_payments,payment_installments,할부 횟수
order_payments,payment_value,결제 금액
order_reviews,review_id,리뷰를 식별하기 위한 고유 ID
order_reviews,order_id,주문을 식별하기 위한 고유 ID
order_reviews,review_score,리뷰 점수(예: 1~5점)
order_reviews,review_comment_title,리뷰 제목
order_reviews,review_comment_message,리뷰 코멘트 내용
order_reviews,review_creation_date,리뷰가 작성된 날짜
order_reviews,review_answer_timestamp,리뷰에 대한 답변이 작성된 날짜 및 시간
orders,order_id,주문을 식별하기 위한 고유 ID
orders,customer_id,고객을 식별하기 위한 고유 ID
orders,order_status,주문 상태(예: delivered, shipped, canceled 등)
orders,order_purchase_timestamp,주문이 생성된 날짜 및 시간
orders,order_approved_at,주문이 승인된 날짜 및 시간
orders,order_delivered_carrier_date,주문이 배송사에 전달된 날짜
orders,order_delivered_customer_date,주문이 고객에게 배송 완료된 날짜
orders,order_estimated_delivery_date,주문의 예상 배송 날짜
products,product_id,상품을 식별하기 위한 고유 ID
products,product_category_name,상품 카테고리 이름(원어)
products,product_name_lenght,상품 이름의 길이(문자 수)
products,product_description_lenght,상품 설명의 길이(문자 수)
products,product_photos_qty,상품 사진 개수
products,product_weight_g,상품 무게(g)
products,product_length_cm,상품 길이(cm)
products,product_height_cm,상품 높이(cm)
products,product_width_cm,상품 너비(cm)
sellers,seller_id,판매자를 식별하기 위한 고유 ID
sellers,seller_zip_code_prefix,판매자의 우편번호 접두사
sellers,seller_city,판매자가 위치한 도시
sellers,seller_state,판매자가 위치한 주(state)
product_category_name_translation,product_category_name,상품 카테고리 이름(원어)
product_category_name_translation,product_category_name_english,상품 카테고리 이름(영어)
"""
    description_list = []
    for line in column_descriptions.strip().split('\n')[1:]:
        file_name, column_name, description = line.split(',', 2)
        description_list.append([file_name, column_name, description])

    df_description = pd.DataFrame(description_list, columns=['enum', 'column_name', 'description'])
    save_path = os.path.join(METADATA_ARTIFACT_DIR, save_file_name)
    df_description.to_csv(save_path, index=False, encoding='utf-8')
    print(f"Done: '{save_path}'")


def save_bronze_paths(save_filename):
    regex_target_file_paths = os.path.join(BRONZE_DIR, '*.csv')
    csv_paths = glob.glob(regex_target_file_paths)
    csv_paths.sort()
    abs_csv_paths = list(map(os.path.abspath, csv_paths))

    file_names = []
    for abs_path in abs_csv_paths:
        _path = os.path.splitext(abs_path)[0]
        file_name = os.path.split(_path)[1]
        file_names.append(file_name)

    processed_name = [
        re.sub(r"^olist_(.*)_dataset$", r"\1", name)
        for name in file_names
    ]
    dataset_var = dict(zip(processed_name, abs_csv_paths))
    with open(os.path.join(METADATA_ARTIFACT_DIR, save_filename), "w") as f:
        json.dump(dataset_var, f, indent=4)

if __name__=="__main__":
    save_bronze_paths('bronze_paths.json')
    save_bronze_relationship("bronze_relationship.json")
    save_bronze_column_description("bronze_column_description.csv")

