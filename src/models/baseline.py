"""
Goal
-------
Evaluate a naive forecasting baseline for demand prediction.

Baseline rule:
prediction=lag_1 (last week's demand)

Why we do this:
Before building ML models (LightGBM, XGBoost),
we must check if a simple rule already performs well.

If ML cannot beat this baseline, then the ML model is useless.
"""

import pandas as pd
import numpy as np
from pathlib import Path

# 1. Load train and test datasets

def load_data():
    """
    Load the processed train and test datasets.
    """
    
    train_path=Path("data/processed/train.parquet")
    test_path=Path("data/processed/test.parquet")
    
    train=pd.read_parquet(train_path)
    test=pd.read_parquet(test_path)
    
    print(f"Train rows: {len(train)}")
    print(f"Test rows: {len(test)}")
    
    return train,test

#2. Create baseline predictions
def make_predictions(test_df):
    """
    Baseline forecast rule"
    preds=lag_1 (last week's demand)
    """
    preds=test_df["lag_1"]
    return preds

#3. Evaluation Metrics

def mean_absolute_error(y_true,y_pred):
    """
    MAE=average (|prediction-actual|)
    """
    return np.mean(np.abs(y_true-y_pred))

def root_mean_squared_error(y_true,y_pred):
    """
    RMSE=sqrt(mean((prediction-actual)^2))
    """
    return np.sqrt(np.mean((y_true-y_pred)**2))

#4. Evaluate overall performance
def evaluate(test_df,preds):
    """
    Evaluate the performance of the baseline model.
    """
    y_true=test_df["target_next_week"]
    mae=mean_absolute_error(y_true,preds)
    rmse=root_mean_squared_error(y_true,preds)
    
    print("\nOverall Performance")
    print("---------------------")   
    print(f"MAE : {mae:.3f}")
    print(f"RMSE: {rmse:.3f}")
    return mae, rmse

#5. Evaluate per-SKU performance

def evaluate_per_sku(test_df,preds):
    df=test_df.copy()
    df["prediction"]=preds
    df["error"]=np.abs(df["target_next_week"]-df["prediction"])
    
    sku_mae=df.groupby("sku_id")["error"].mean().sort_values(ascending=False)
    
    print("\n Worst SKUS by MAE")
    print("---------------------") 
    print(sku_mae.head(10))
    
    return sku_mae

#6 Main pipeline

def main():
    # Step 1
    train,test=load_data()
    
    # Step 2
    preds=make_predictions(test)
    
    #step 3
    evaluate(test,preds)
    
    #step 4
    evaluate_per_sku(test,preds)
    

if __name__=="__main__":
    main()