# import requests

# BASE = "http://localhost:8080"

# token = requests.post(
#     f"{BASE}/auth/token",
#     json={"username":"airflow", "password": "airflow"},
#     )

# jwt = token.json()['access_token']

# headers = {"Authorization": f"Bearer {jwt}"}
# request = requests.get(f"{BASE}/api/v2/dags?only_active=true", headers=headers)

# for dag in request.json().get('dags', []):
#     print(dag['dag_id'])