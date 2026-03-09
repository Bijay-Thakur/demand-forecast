import pandas as pd
import numpy as np
from pathlib import Path
import lightgbm as lgb
from sklearn.metrics import mean_absolute_error, mean_squared_error

FEATURE_COLS = [
    "lag_1",
    "lag_2",
    "lag_4",
    "roll_mean_4",
    "roll_mean_8",
    "avg_net_price",
    "avg_discount_pct",
    "promo_flag_week",
    "month",
    "weekofyear",
    "sku_id",
]

TARGET_COL = "target_next_week"


def load_data():
    train_path = Path("data/processed/train.parquet")
    test_path = Path("data/processed/test.parquet")

    train = pd.read_parquet(train_path)
    test = pd.read_parquet(test_path)

    print(f"Train rows: {len(train)}")
    print(f"Test rows: {len(test)}")

    return train, test


def prepare_data(train, test):
    train = train.copy()
    test = test.copy()

    # Ensure sku_id is categorical
    train["sku_id"] = train["sku_id"].astype("category")
    test["sku_id"] = test["sku_id"].astype("category")
    test["sku_id"] = test["sku_id"].cat.set_categories(train["sku_id"].cat.categories)

    X_train = train[FEATURE_COLS]
    y_train = train[TARGET_COL]

    X_test = test[FEATURE_COLS]
    y_test = test[TARGET_COL]

    return X_train, y_train, X_test, y_test, test


def build_lgb_datasets(X_train, y_train, X_test, y_test):
    categorical_features = ["sku_id"]

    lgb_train = lgb.Dataset(
        X_train,
        label=y_train,
        categorical_feature=categorical_features,
        free_raw_data=False,
    )

    lgb_valid = lgb.Dataset(
        X_test,
        label=y_test,
        categorical_feature=categorical_features,
        free_raw_data=False,
    )

    return lgb_train, lgb_valid


def get_lgb_params():
    params = {
        "objective": "regression",
        "metric": "rmse",
        "boosting_type": "gbdt",
        "learning_rate": 0.05,
        "num_leaves": 31,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.8,
        "bagging_freq": 1,
        "verbose": -1,
    }
    return params


def train_model(lgb_train, lgb_valid, params):
    evals_result = {}

    model = lgb.train(
        params,
        lgb_train,
        num_boost_round=1000,
        valid_sets=[lgb_train, lgb_valid],
        valid_names=["train", "valid"],
        evals_result=evals_result,
        early_stopping_rounds=50,
        verbose_eval=50,
    )

    print(f"Best iteration: {model.best_iteration}")
    return model


def evaluate_model(model, X_test, y_test):
    preds = model.predict(X_test, num_iteration=model.best_iteration)

    mae = mean_absolute_error(y_test, preds)
    rmse = mean_squared_error(y_test, preds, squared=False)

    print("\nLightGBM Performance on Test Set")
    print("--------------------------------")
    print(f"MAE : {mae:.3f}")
    print(f"RMSE: {rmse:.3f}")

    return preds, mae, rmse


def evaluate_per_sku(test_df, preds):
    df = test_df.copy()
    df["prediction"] = preds
    df["error"] = np.abs(df["target_next_week"] - df["prediction"])

    sku_mae = df.groupby("sku_id")["error"].mean().sort_values(ascending=False)

    print("\nWorst SKUs by MAE (LightGBM)")
    print("-----------------------------")
    print(sku_mae.head(10))

    return sku_mae


def save_model(model, path="models/lightgbm_demand.txt"):
    model_path = Path(path)
    model_path.parent.mkdir(parents=True, exist_ok=True)
    model.save_model(str(model_path))
    print(f"Saved model to {model_path}")


def main():
    train, test = load_data()
    X_train, y_train, X_test, y_test, test_df = prepare_data(train, test)

    lgb_train, lgb_valid = build_lgb_datasets(X_train, y_train, X_test, y_test)
    params = get_lgb_params()
    model = train_model(lgb_train, lgb_valid, params)

    preds, mae, rmse = evaluate_model(model, X_test, y_test)
    evaluate_per_sku(test_df, preds)

    save_model(model)


if __name__ == "__main__":
    main()