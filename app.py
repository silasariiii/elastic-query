import json
from flask import Flask, Response
from elasticsearch import Elasticsearch
app = Flask(__name__)
es = Elasticsearch("http:...")
@app.route('/data', methods=['GET'])
def get_data():
    response = es.search(index="jaeger-span-2024-07-22", body={
        "query": {
            "bool": {
                "must": [
                    {"match": {"operationName": "POST Transactions/Transfer"}},
                    {"match": {"process.serviceName": "dotnet-bank-api"}},
                    {
                        "nested": {
                            "path": "tags",
                            "query": {
                                "bool": {
                                    "must": [
                                        {"match": {"tags.key": "http.response.body"}}
                                    ]
                                }
                            }
                        }
                    }
                ]
            }
        },
        "_source": ["operationName", "tags", "duration"],
        "from": 0,
        "size": 1000
    })
    
    data = []
    for hit in response['hits']['hits']:
        tags = hit['_source']['tags']
        duration = hit['_source'].get('duration', 0)
        for tag in tags:
            if tag['key'] == 'http.response.body':
                try:
                    response_body = json.loads(tag['value'])
                except json.JSONDecodeError as e:
                    print(f"JSONDecodeError: {e}")
                    print(f"Value causing error: {tag['value']}")
                    continue
                sender_customer = response_body.get('SenderCustomerId', 'Unknown')
                sender_account = response_body.get('SenderAccountId', 'Unknown')
                receiver_customer = response_body.get('ReceiverCustomerId', 'Unknown')
                receiver_account = response_body.get('ReceiverAccountId', 'Unknown')
                amount = response_body.get('Amount', 0)
                time = response_body.get('Time', 'Unknown')

                data.append({
                    'userId': sender_customer,
                    'accountNumber': sender_account,
                    'total_spent': amount,
                    'total_received': 0,
                    'transaction_count': 1,
                    'time': time,
                    'duration_ms': duration / 1000 
                })
                
                data.append({
                    'userId': receiver_customer,
                    'accountNumber': receiver_account,
                    'total_spent': 0,
                    'total_received': amount,
                    'transaction_count': 1,
                    'time': time,
                    'duration_ms': duration / 1000  
                })

    return Response(json.dumps(data), mimetype='application/json')

@app.route('/errors', methods=['GET'])
def get_all_errors():
    response = es.search(index="jaeger-span-2024-07-23", body={
        "query": {
            "bool": {
                "must": [
                    {"match": {"process.serviceName": "dotnet-bank-api"}},
                    {
                        "nested": {
                            "path": "tags",
                            "query": {
                                "bool": {
                                    "must": [
                                        {"match": {"tags.key": "http.response.body"}}
                                    ]
                                }
                            }
                        }
                    }
                ]
            }
        },
        "_source": ["operationName", "tags", "duration"],
        "from": 0,
        "size": 1000
    })

    error_data = []
    for hit in response['hits']['hits']:
        tags = hit['_source']['tags']
        duration = hit['_source'].get('duration', 0)
        for tag in tags:
            if tag['key'] == 'http.response.body':
                try:
                    response_body = json.loads(tag['value'])
                    if response_body.get('error_no') == 201:
                        error_data.append({
                            'operationName': hit['_source'].get('operationName', 'Unknown'),
                            'response_body': response_body,
                            'duration_ms': duration / 1000
                        })
                    
                except json.JSONDecodeError as e:
                    print(f"JSONDecodeError: {e}")
                    print(f"Value causing error: {tag['value']}")
                    continue

    return Response(json.dumps(error_data), mimetype='application/json')


@app.route('/errors/<int:bank_id>', methods=['GET'])
def get_errors_by_bank(bank_id):
    response = es.search(index="jaeger-span-2024-07-22", body={
        "query": {
            "bool": {
                "must": [
                    {"match": {"operationName": "POST Transactions/Transfer"}},
                    {"match": {"process.serviceName": "dotnet-bank-api"}},
                    {
                        "nested": {
                            "path": "tags",
                            "query": {
                                "bool": {
                                    "must": [
                                        {"match": {"tags.key": "http.response.body"}}
                                    ]
                                }
                            }
                        }
                    }
                ]
            }
        },
        "_source": ["operationName", "tags", "duration"],
        "from": 0,
        "size": 1000
    })

    bank_errors = []
    for hit in response['hits']['hits']:
        tags = hit['_source']['tags']
        duration = hit['_source'].get('duration', 0)
        for tag in tags:
            if tag['key'] == 'http.response.body':
                try:
                    response_body = json.loads(tag['value'])
                    sender_bank = response_body.get('SenderBankId')
                    receiver_bank = response_body.get('ReceiverBankId')

                    if (sender_bank == bank_id or receiver_bank == bank_id) and response_body.get('error_no') == 201:
                        bank_errors.append({
                            'error_no': response_body.get('error_no'),
                            'message': response_body.get('message'),
                            'duration_ms': duration / 1000
                        })
                    
                except json.JSONDecodeError as e:
                    print(f"JSONDecodeError: {e}")
                    print(f"Value causing error: {tag['value']}")
                    continue

    return Response(json.dumps(bank_errors), mimetype='application/json')

@app.route('/time', methods=['GET'])
def get_time_data():
    response = es.search(index="jaeger-span-2024-07-22", body={
        "query": {
            "bool": {
                "must": [
                    {"match": {"operationName": "POST Transactions/Transfer"}},
                    {"match": {"process.serviceName": "dotnet-bank-api"}}
                ]
            }
        },
        "_source": ["operationName", "duration"],
        "from": 0,
        "size": 1000
    })

    time_data = []
    for hit in response['hits']['hits']:
        duration = hit['_source'].get('duration', 0)
        time_data.append({
            'duration_ms': duration / 1000  # milisaniye cinsinden göster
        })

    return Response(json.dumps(time_data), mimetype='application/json')

@app.route('/percentiles', methods=['GET'])
def get_percentiles():
    response = es.search(index="jaeger-span-2024-07-22", body={
        "size": 0,
        "query": {
            "bool": {
                "filter": [
                    {"terms": {
                        "operationName": [
                            "POST Transactions/Transfer",
                            "POST /api/v1/transactions/fee",
                            "POST /api/v1/transactions/deposit",
                            "POST /api/v1/transactions/withdraw",
                            "POST /api/v1/transactions/refund",
                            "POST /api/v1/transactions/payment"
                        ]
                    }}
                ]
            }
        },
        "aggs": {
            "by_operation": {
                "terms": {
                    "field": "operationName",
                    "size": 10
                },
                "aggs": {
                    "load_time_percentiles": {
                        "percentiles": {
                            "field": "duration",
                            "percents": [50, 75, 90, 95, 99]
                        }
                    }
                }
            }
        }
    })

    percentiles_data = {}
    for bucket in response['aggregations']['by_operation']['buckets']:
        operation_name = bucket['key']
        percentiles = bucket['load_time_percentiles']['values']
        percentiles_data[operation_name] = {str(k): v / 1000 for k, v in percentiles.items()}  # milisaniye cinsinden göster

    return Response(json.dumps(percentiles_data), mimetype='application/json')

@app.route('/slowest', methods=['GET'])
def get_slowest_operations():
    response = es.search(index="jaeger-span-2024-07-22", body={
        "query": {
                        "bool": {
                "must": [
                    {"match": {"process.serviceName": "dotnet-bank-api"}}
                ]
            }
        },
        "_source": ["operationName", "duration"],
        "from": 0,
        "size": 1000,
        "sort": [
            {"duration": {"order": "desc"}}
        ]
    })

    slowest_operations = []
    for hit in response['hits']['hits'][:5]:  # en yavaş 5 işlemi al
        operation_name = hit['_source'].get('operationName', 'Unknown')
        duration = hit['_source'].get('duration', 0)
        slowest_operations.append({
            'operationName': operation_name,
            'duration_ms': duration / 1000  # milisaniye cinsinden göster
        })

    return Response(json.dumps(slowest_operations), mimetype='application/json')

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=.... , debug=False)
