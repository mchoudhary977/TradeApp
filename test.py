from flask import Flask, render_template, jsonify
import pandas as pd
from datetime import datetime

app = Flask(__name__)

@app.route('/get_data')
def get_data():
    # Simulating a dictionary of DataFrames with updated data every second
    resulDict = {
        'WatchList' : pd.read_csv('WatchList.csv')}
    # data = {
    #     # 'df1': pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]}),
    #     'df2': pd.DataFrame({'X': ['x', 'y', 'z'], 'Y': ['a', 'b', 'c']})
    # }

    resulDict['WatchList']['Time'] = datetime.now().strftime('%H:%M:%S')

    # Convert DataFrames to JSON-formatted dictionaries
    json_data = {key: df.to_dict(orient='records') for key, df in resulDict.items()}

    return jsonify(json_data)

@app.route('/')
def home():
    return render_template('test.html')

df = pd.read_csv('WatchList.csv')
if __name__ == '__main__':
    app.run()
