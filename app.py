from chalice import Chalice

app = Chalice(app_name='chalice-api-sample')


@app.route('/')
def inx():
    return "helloworld"

@app.route('/{name}')
def users(name):
    return {'name': name}

@app.route('/hello/{name}')
def hello_name(name):
   # '/hello/james' -> {"hello": "james"}
   return {'hello': name}

