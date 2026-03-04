"""
Goal
1. Build an ML- ready dataset from core.sku-weekly
2. create time series features (lags, rolling means)
3. Create target i.e next week demand
4. Split into train/test by time (no leakage)
5. Save to data/processed/

how to run:
python -m src.features.make_dataset --min_weeks 8 --test_weeks 6
"""

from __future__ import annotations
import os
import argparse
from dataclasses import dataclass
from pathlib import Path
# from dotenv import main
import numpy as np
import pandas as pd

# I already have src/db/connection.py in my repo
from src.db.connection import get_engine


@dataclass
class DatasetConfig:
    min_weeks: int = 8  # keep SKUs with at least this many weeks
    test_weeks: int = 6  # last N weeks reserved for test
    out_dir: str = "data/processed"


def load_weekly_table() -> pd.DataFrame:
    """
    Reads weekly SKU table from Postgress into Pandas.

    Why:
    -SQL is my source of truth for stored data
    -Pandas is where I do feauture engineering for ML
    """

    engine = get_engine()
    query = """
    SELECT week_start,
        sku_id,
        units_week,
        avg_net_price,
        avg_discount_pct,
        promo_flag_week
    
    FROM core.sku_weekly
    ORDER BY sku_id, week_start
    
    """

    df = pd.read_sql(query, con=engine)
    return df


def filter_skus_with_history(df: pd.DataFrame, min_weeks: int) -> pd.DataFrame:
    """
    keep only the SKU_s that have enough history to compute lag/rolling features.

    Why:
    -If a SKU has 2 weeks of sales, you cannot compute rolling 8-week averages.
    -For a first model, we prefer SKUs with signal and history.
    """

    counts = df.groupby("sku_id")["week_start"].count()
    good_skus = counts[counts >= min_weeks].index
    return df[df["sku_id"].isin(good_skus)].copy()


def make_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create times-series features + targets

    Key ideas:
    - Sort by (sku_id, week start) so "previous row" really means "previous weeks"
    - groupby(sku_id) so shifts/rolling operate within a SKU, not across SKUs
    Use shift(1) before rolling to avoid including current week in the window
    """
    df = df.copy()

    # Ensure proper datetime & order

    df["week_start"] = pd.to_datetime(df["week_start"])
    df = df.sort_values(["sku_id", "week_start"]).reset_index(drop=True)

    g = df.groupby("sku_id", group_keys=False)

    # ---Lag features  (past demand)--
    df["lag_1"] = g["units_week"].shift(1)
    df["lag_2"] = g["units_week"].shift(2)
    df["lag_4"] = g["units_week"].shift(4)

    # ---Rolling features (smoothed demand level / trend)
    # IMPORTANT" shift(1) first=only past weeks enter the rolling window
    df["roll_mean_4"] = g["units_week"].shift(1).rolling(4).mean()
    df["roll_mean_8"] = g["units_week"].shift(1).rolling(8).mean()

    # --- Date features (Seasonality signals)
    df["month"] = df["week_start"].dt.month
    df["weekofyear"] = df["week_start"].dt.isocalendar().week.astype(int)

    # --- target:next week's demand (1-week horizon)
    df["target_next_week"] = g["units_week"].shift(-1)

    return df


def drop_untrainable_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows that cannnot be used for training.

    Why rows become unusable:
    - early weeks don't have enough history for lag/rolling
    - last week has no "nex week" target
    """

    feature_cols = ["lag_1", "lag_2", "lag_4", "roll_mean_4", "roll_mean_8",
                    "avg_net_price", "avg_discount_pct", "promo_flag_week", "month", "weekofyear",]

    df_model = df.dropna(subset=feature_cols+["target_next_week"]).copy()

    # optional: enforce integer-like units
    df_model["units_week"] = df_model["units_week"].astype(float)
    df_model["target_next_week"] = df_model["target_next_week"].astype(float)
    return df_model


def time_split(df_model: pd.dataFrame, test_weeks: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Time-based splits: last N weeks = test

    Why:
    - Forecasting must simulate real life: train on past, test on future.
    - Random split woruld leak future patterns into training
    """

    max_week = df_model["week_start"].max()
    cutoff = max_week-pd.Timedelta(weeks=test_weeks)
    train = df_model[df_model["week_start"] <= cutoff].copy()
    test = df_model[df_model["week_start"] > cutoff].copy()

    return train, test


def save_datasets(train: pd.DataFrame, test: pd.DataFrame, out_dir: str) -> None:
    """
Saves train/test to disk.

Why:
- Models should train from saved, versioned datasets.
- Makes pipeline reproducible and fast.
    """

    out_path=Path(out_dir)
    out_path.mkdir(parents=True,exist_ok=True)

    train_path=out_path/"train.parquet"
    test_path=out_path/"test.parquet"

    train.to_parquet(train_path,index=False)
    test.to_parquet(test_path,index=False)
    
    print(f"Saved train: {train_path} rows={len(train):,}")
    print(f"Saved test: {test_path} rows={len(test):,}")
    
    
def main():
        parser=argparse.ArgumentParser()
        parser.add_argument("--min_weeks",type=int,default=8,help="Minimum week of history per SKU")
        parser.add_argument("--test_weeks",type=int,default=6,help="Number of weeks to reserve for test set")
        parser.add_argument("--out_dir",type=str,default="data/processed",help="Output director for processed datasets")
        args=parser.parse_args()
        
        cfg=DatasetConfig(min_weeks=args.min_weeks,test_weeks=args.test_weeks,out_dir=args.out_dir)
        
        #1) Load
        df=load_weekly_table()
        print(f"Loaded core.sku_weekly rows={len(df):,} skus={df['sku_id'].nunique():,}")
        #2) Filter SKUS with enough history
        df=filter_skus_with_history(df,cfg.min_weeks)
        print(f"After filter(min_weeks={cfg.min_weeks}) rows={len(df):,} skus={df['sku_id'].nunique():,}")
        
        #3) Feature engineering
        df_feat=make_features(df)
        
        #4) Drop rows we cannot train on
        df_model = drop_untrainable_rows(df_feat)
        print(f"After drop_untrainable_rows rows={len(df_model):,} skus={df_model['sku_id'].nunique():,}")
        
         # 5) Time split
        train, test = time_split(df_model, cfg.test_weeks)
        print(f"Train rows={len(train):,} | Test rows={len(test):,}")
        print(f"Train weeks: {train['week_start'].min().date()} -> {train['week_start'].max().date()}")
        print(f"Test  weeks: {test['week_start'].min().date()} -> {test['week_start'].max().date()}")

        # 6) Save
        save_datasets(train, test, cfg.out_dir)


if __name__ == "__main__":
    main()