import sys
import os
import xgboost as xgb
import pandas as pd

def main():
    if len(sys.argv) < 7:
        print(100 if len(sys.argv) > 1 and sys.argv[1] == 'batch' else 1)
        return
        
    target_type = sys.argv[1]
    fallback = 100 if target_type == 'batch' else 1
    
    try:
        model_filename = 'batch_size_model.json' if target_type == 'batch' else 'pipeline_depth_model.json'
        model_path = os.path.join('ml', 'models', model_filename)
        
        if not os.path.exists(model_path):
            print(fallback)
            return

        features = {
            'total_ops': [float(sys.argv[2])],
            'throughput_ops_sec': [float(sys.argv[3])],
            'avg_latency_ms': [float(sys.argv[4])],
            'p99_latency_ms': [float(sys.argv[5])],
            'write_amplification': [float(sys.argv[6])]
        }
        df = pd.DataFrame(features)
        
        model = xgb.XGBRegressor()
        model.load_model(model_path)
        
        pred = model.predict(df)[0]
        recommended = int(round(pred))
        
        # Safety Bounds Validation
        if target_type == 'batch':
            if recommended < 10: recommended = 10
            elif recommended > 500: recommended = 500
        else:
            if recommended < 1: recommended = 1
            elif recommended > 16: recommended = 16
            
        print(recommended)
    except Exception:
        print(fallback)

if __name__ == "__main__":
    main()