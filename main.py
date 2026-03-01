from src.extract import extract_bank
from src.transform import transform_bank
from src.analytics import analytics_check

if __name__ == '__main__':
    analytics_check(transform_bank(extract_bank()))