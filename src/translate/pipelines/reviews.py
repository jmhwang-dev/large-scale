import os
import json
from pathlib import Path
import re
from typing import List, Union

import emoji
import pandas as pd

# from translate.utils.paths import PREPROCESS_ARTIFACTS_DIR

def replace_emoji_with_dot(text):
    if not isinstance(text, str):
        return text
    text = emoji.replace_emoji(text, replace='.')
    return text

def replace_emoticon_with_dot(text):
    if not isinstance(text, str):
        return text
    
    pattern = r'(:\)|;\)|:-\)|:\(|:\\|:/|\^_+\^|\*__*\*)'
    
    def replacer(match):
        matched = match.group(0)
        if matched == ':/':
            start = match.start()
            # Check up to 8 chars before ':/' for 'http://' or 'https://'
            context = text[max(0, start - 8):start].lower()
            if 'http' in context and context.endswith('http'):
                return matched  # Keep ':/' as part of URL
            else:
                return '.'
        else:
            return '.'
    
    return re.sub(pattern, replacer, text)

def convert_emphasis_to_single_quote(text):
    if not isinstance(text, str):
        return text
    return re.sub(r'[`´"“”‘’\'*^]', "'", text)

def replace_exclamation_with_dot(text):
    if not isinstance(text, str):
        return text
    return text.replace('!', '.')

def remove_trailing_punctuation(text):
    if not isinstance(text, str):
        return text
    return re.sub(r'[.,+]+$', '', text)

def reduce_repeated_special_chars(text):
    # Avoid reducing '//' after 'http:' or 'https:'
    def replacer(match):
        s = match.group(0)
        if s.startswith('//'):
            start = match.start()
            # Check if '//' comes after 'http:' or 'https:'
            if start >= 6:  # minimum length for 'https:'
                before = text[start-6:start]
                if before.endswith('https:'):
                    return s  # keep '//' after https:
            if start >= 5:  # minimum length for 'http:'
                before = text[start-5:start]
                if before.endswith('http:'):
                    return s  # keep '//' after http:
        return s[0]  # reduce to single character
    
    return re.sub(r'([^\w\s])\1+', replacer, text)

def reduce_repeated_spaces(text):
    if not isinstance(text, str):
        return text
    # 공백이 여러 개일 경우 하나로 줄임
    text = re.sub(r'\s+', ' ', text)
    return text.strip()  # 양쪽 여백 제거

def remove_space_before_punctuation(text):
    if not isinstance(text, str):
        return text
    # 느낌표, 물음표, 마침표, 쉼표 앞의 공백 제거
    return re.sub(r'\s+([!?.,])', r'\1', text)

def remove_single_quote(text):
    if not isinstance(text, str):
        return text
    
    # 홑따옴표 개수 세기
    count = text.count("'")
    if count == 1:
        # 홑따옴표 1개면 모두 제거
        return text.replace("'", "")
    else:
        # 그 외는 그대로 반환
        return text
    
def reduce_repeated_chars(text):
    if not isinstance(text, str):
        return text
    # 같은 문자가 3번 이상 반복되면 하나로 치환
    return re.sub(r'(.)\1{2,}', r'\1', text)


def strip_spaces_inside_quotes(text):
    if not isinstance(text, str):
        return text
    
    def replacer(m):
        return f"'{m.group(1).strip()}'"
    
    return re.sub(r"'(.*?)'", replacer, text, flags=re.DOTALL)

def remove_if_short(text):
    if not isinstance(text, str):
        return text
    if len(text) <= 2:
        return ''
    return text

def remove_empty_or_whitespace_single_quotes(text):
    if not isinstance(text, str):
        return text
    return re.sub(r"'\s*'", '', text)

class PortuguessPreprocessor:
    def __init__(
        self,
        dataset: pd.DataFrame,
        target_cols: List[str],
        manual_fix_json_path: str = '',
        value_column_name: str = 'comment'
    ):
        self.dataset = dataset
        self.target_cols = target_cols
        self.value_column_name = value_column_name
        self.manual_fix_json_path = manual_fix_json_path
        self.manual_fix_data = self._load_manual_fix_data()
        self.processed_df = None

    def _load_manual_fix_data(self) -> dict:
        if len(self.manual_fix_json_path) == 0:
            return {}
        with open(self.manual_fix_json_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    @staticmethod
    def melt_reviews(df:pd.DataFrame) -> pd.DataFrame:
        if 'review_id' not in df.columns:
            raise ValueError("Column 'review_id' must exist in dataset")
        melted_df = df.melt(
            id_vars='review_id',
            var_name='column_name',
            value_name='portuguess'
        )
        melted_df.dropna(inplace=True)
        melted_df.reset_index(drop=True, inplace=True)
        return melted_df

    @staticmethod
    def clean_review_comment(df: pd.DataFrame, value_column_name: str ='portuguess') -> pd.DataFrame:
        df[value_column_name] = df[value_column_name].str.lower()
        df[value_column_name] = df[value_column_name].str.replace(r'[\r\n]', ' ', regex=True)
        df[value_column_name] = df[value_column_name].apply(replace_emoji_with_dot)
        df[value_column_name] = df[value_column_name].apply(replace_emoticon_with_dot)

        df[value_column_name] = df[value_column_name].apply(convert_emphasis_to_single_quote)
        df[value_column_name] = df[value_column_name].apply(remove_empty_or_whitespace_single_quotes)
        df[value_column_name] = df[value_column_name].apply(remove_single_quote)

        df[value_column_name] = df[value_column_name].apply(reduce_repeated_special_chars)
        df[value_column_name] = df[value_column_name].apply(replace_exclamation_with_dot)
        df[value_column_name] = df[value_column_name].apply(remove_space_before_punctuation)

        df[value_column_name] = df[value_column_name].apply(reduce_repeated_chars)
        df[value_column_name] = df[value_column_name].apply(reduce_repeated_spaces)
        df[value_column_name] = df[value_column_name].apply(remove_trailing_punctuation)
        
        df[value_column_name] = df[value_column_name].apply(remove_if_short)
        df[value_column_name] = df[value_column_name].apply(strip_spaces_inside_quotes)
        
        df[value_column_name] = df[value_column_name].str.strip()

        df = df[df[value_column_name].notna() & (df[value_column_name].str.strip() != '')]
        df = df.reset_index(drop=True)
        return df

    def manual_fix(self, df: pd.DataFrame, target_col: str) -> pd.DataFrame:
        if len(self.manual_fix_data) == 0:
            print('Not setted manual fix param')
            return df
        fixed_contents = self.manual_fix_data.get(target_col, {})
        log_path = os.path.join(PREPROCESS_ARTIFACTS_DIR, f"before_fix_{target_col}.tsv")
        df[df['review_id'].isin(fixed_contents.keys())].to_csv(log_path, sep='\t', index=False)

        for review_id, fixed_content in fixed_contents.items():
            df.loc[df['review_id'] == review_id, self.value_column_name] = fixed_content

        df = df[df[self.value_column_name].str.strip() != ''].reset_index(drop=True)
        return df

    def fix_reviews(self, df: pd.DataFrame) -> pd.DataFrame:
        target_types = [col for col in self.target_cols if col != 'review_id']
        for col_name in target_types:
            df = self.manual_fix(df, col_name)
        return df

    def run(self, dst_path: Union[str, Path]) -> pd.DataFrame:
        melted_df = PortuguessPreprocessor.melt_reviews(self.dataset)
        cleaned_df = PortuguessPreprocessor.clean_review_comment(melted_df)
        fixed_df = self.fix_reviews(cleaned_df)
    
        fixed_df['comment_length'] = fixed_df[self.value_column_name].str.len()
        fixed_df = fixed_df.sort_values(by='comment_length', ascending=False)
        fixed_df.drop(columns=['comment_length'], inplace=True)
        fixed_df = fixed_df.drop_duplicates()

        dst_path = Path(dst_path)
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        fixed_df.to_csv(dst_path, sep='\t', index=False)

        self.processed_df = fixed_df
        return fixed_df

# TODO: Consider consolidating into a class: `is_conflict`, `gather_inference`
from translate.utils.paths import INFERENCE_CONFIGS_DIR
from translate.utils.config import PipelineConfig, PostProcessConfig
from translate.utils.loader import get_dataset

def is_conflict(df:pd.DataFrame):
    # p2e별로 max_sentimental 값이 몇 종류 있는지 계산
    label_conflicts = (
        df
        .groupby('por2eng')['sentimentality']
        .nunique()
        .gt(1)  # 그룹 내 감성 레이블이 2개 이상이면 True
    )
    conflict_count = len(label_conflicts[label_conflicts == True])
    if conflict_count > 0:
        print("Conflict exists!")
        return True
    return False

def gather_inference(sent_config_file_name: str, trans_config_file_name: str) -> pd.DataFrame:
    senti_config_path = Path(INFERENCE_CONFIGS_DIR) / sent_config_file_name
    senticonfig = PipelineConfig.load(senti_config_path)
    senti_df, _ = get_dataset(senticonfig.dst_path)

    trans_config_path = Path(INFERENCE_CONFIGS_DIR) / trans_config_file_name
    trans_config = PostProcessConfig.load(trans_config_path)
    trans_df, _ = get_dataset(trans_config.dst_path)

    gather_df = trans_df.copy()
    gather_df['sentimentality'] = senti_df.drop(columns=['por2eng']).idxmax(axis=1)
    return gather_df
