from flask import Flask, render_template
from pathlib import Path

dir_curr = Path(__file__).parent
dir_template = Path(f'{dir_curr}/_build/html/')
dir_stat = Path(f'{dir_template}/_static')

app = Flask(__name__,
            template_folder=str(dir_template),
            static_folder=dir_stat)


@app.route('/<path:path>')
def documentation(path):
    return render_template(path)


if __name__ == '__main__':
    app.run(host="localhost", threaded=True, port=5000)
