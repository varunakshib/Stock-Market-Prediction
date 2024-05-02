from flask import Flask, render_template, redirect, request, jsonify
from utils import predict_price
from pyngrok import ngrok

app = Flask(__name__)
ngrok.set_auth_token("2fuDO1InjqZ1rA4sLo5UyLBT5NG_54V1C7wAE9UdwoBfR2gzg")
public_url = ngrok.connect(5000).public_url

print(public_url)

@app.route("/")
def index():
    return render_template('index.html')

@app.route('/receive-data', methods=['POST'])
def receive_data():
    if request.method == 'POST':
        card_name = request.form.get('cardName')
        selected_date = request.form.get('selectedDate')
        print('Received data - Card Name:', card_name, 'Selected Date:', selected_date)

        # Assuming predict_price is defined elsewhere and returns data
        prediction_data = predict_price(selected_date, card_name)

        # Do whatever you want with the received data
        print('Received data - Card Name:', card_name, 'Selected Date:', selected_date)

        # Render the prediction page with the received data
        return render_template('prediction.html', data=prediction_data)
        # return "success"

    # In case of GET request or any other method, return an error or redirect
    return "Invalid request"

app.run(host='0.0.0.0', port=5000)
# if __name__ == "__main__":
#     app.run(debug = True)