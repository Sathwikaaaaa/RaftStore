import os
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import GroupShuffleSplit
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

def closest_category(pred, categories):
    return min(categories, key=lambda x: abs(x - pred))

def evaluate_metrics(y_true, y_pred, target_name, categories):
    rmse = mean_squared_error(y_true, y_pred) ** 0.5
    mae = mean_absolute_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)
    
    y_pred_cat = [closest_category(p, categories) for p in y_pred]
    y_true_list = list(y_true)
    exact_matches = sum(1 for i in range(len(y_true_list)) if y_pred_cat[i] == y_true_list[i])
    accuracy = (exact_matches / len(y_true_list)) * 100

    print(f"\n[{target_name}] Evaluation Metrics:")
    print(f"  - RMSE:                 {rmse:.4f}")
    print(f"  - MAE:                  {mae:.4f}")
    print(f"  - R-squared (Variance): {r2:.4f}")
    print(f"  - Categorical Accuracy: {accuracy:.2f}% (Target: >75%)")

def train_model(X, y, groups, target_name, model_path, valid_categories):
    print(f"\n--- Training & Evaluating: {target_name} ---")
    
    gss = GroupShuffleSplit(n_splits=1, train_size=0.75, random_state=42)
    train_idx, test_idx = next(gss.split(X, y, groups))
    
    X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
    y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
    
    model = xgb.XGBRegressor(
        objective='reg:squarederror',
        n_estimators=200,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42
    )
    
    model.fit(X_train, y_train)
    predictions = model.predict(X_test)
    
    evaluate_metrics(y_test, predictions, target_name, valid_categories)

    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    model.save_model(model_path)

def main():
    dataset_path = 'results/raftstore_workload_dataset.csv'
    if not os.path.exists(dataset_path):
        print("Dataset not found.")
        return

    df = pd.read_csv(dataset_path)
    print(f"\nLoaded {len(df)} raw experimental observations.")

    # 1. Evaluate Performance to find the OPTIMAL configuration per scenario
    # Maximize throughput while penalizing high tail latency
    df['performance_score'] = df['throughput'] / (df['p95_latency'] + 1)
    
    scenario_cols = ['workload_type', 'avg_value_size', 'client_threads']
    
    # Find the row index with the highest performance score for each scenario
    best_idx = df.groupby(scenario_cols)['performance_score'].idxmax()
    best_configs = df.loc[best_idx]
    
    # Create mapping of scenario -> best config
    best_map = best_configs.set_index(scenario_cols)[['batch_size', 'pipeline_depth']]
    best_map = best_map.rename(columns={'batch_size': 'optimal_batch', 'pipeline_depth': 'optimal_pipeline'})
    
    # Merge the optimal labels back into the main dataset
    df = df.merge(best_map, on=scenario_cols, how='left')

    # 2. One-Hot Encode Categorical Workload Types (A-F)
    df = pd.get_dummies(df, columns=['workload_type'])

    # 3. Define Final Input Features
    feature_cols = [
        'avg_value_size', 'client_threads', 'replication_latency', 
        'follower_lag', 'network_rtt', 'cpu_utilization', 'memory_utilization'
    ]
    # Include all the one-hot encoded workload type columns
    feature_cols += [col for col in df.columns if col.startswith('workload_type_')]
    
    X = df[feature_cols]
    groups = df['experiment_id']

    # 4. Train using the OPTIMAL labels, not the randomly applied ones
    valid_batch = [16, 32, 64, 128, 256]
    train_model(X, df['optimal_batch'], groups, "MaxBatchSize", "ml/models/batch_size_model.json", valid_batch)

    valid_pipe = [1, 2, 4, 8, 16]
    train_model(X, df['optimal_pipeline'], groups, "MaxPipelineDepth", "ml/models/pipeline_depth_model.json", valid_pipe)

if __name__ == "__main__":
    main()