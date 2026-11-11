import os
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

def main():
    dataset_path = 'results/dataset_train.csv'
    model_path = 'ml/models/batch_size_xgboost.json'

    print("=== RaftStore XGBoost Training Pipeline ===")
    
    if not os.path.exists(dataset_path):
        print(f"Error: Dataset not found at {dataset_path}")
        return

    # 1. Load the dataset
    print(f"Loading dataset from {dataset_path}...")
    df = pd.read_csv(dataset_path)
    
    print(f"Total samples loaded: {len(df)}")

    # 2. Define Features (X) and Target (y)
    features = [
        'total_ops', 
        'throughput_ops_sec', 
        'avg_latency_ms', 
        'p99_latency_ms', 
        'write_amplification'
    ]
    
    target = 'max_batch_size'

    X = df[features]
    y = df[target]

    # 3. Split into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # 4. Initialize and train the XGBoost Regressor
    print("Training XGBoost model...")
    model = xgb.XGBRegressor(
        objective='reg:squarederror',
        n_estimators=100,
        learning_rate=0.1,
        max_depth=5,
        random_state=42
    )
    
    model.fit(X_train, y_train)

    # 5. Evaluate the model using native square root
    predictions = model.predict(X_test)
    mse = mean_squared_error(y_test, predictions)
    rmse = mse ** 0.5
    print(f"Model Evaluation - Root Mean Squared Error (RMSE): {rmse:.4f}")

    # 6. Save the model for the Go runtime controller to use later
    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    model.save_model(model_path)
    print(f"Model successfully saved to {model_path}")

if __name__ == "__main__":
    main()