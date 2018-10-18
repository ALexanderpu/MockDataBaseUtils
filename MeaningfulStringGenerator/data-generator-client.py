import requests


if __name__ == "__main__":

    url = 'http://localhost:8000/api/process.php?request=data'
    with open('request.json', 'r') as file:
        data = file.read()

    response = requests.post(url, data=data)
    with open('data.csv', 'wb') as file:
        file.write(response.content)

