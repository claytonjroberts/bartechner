function pause() {
    read -s -k '?Press any key to continue.'
}

function runpython() {
    # pause
    export FLASK_APP=__main__.py
    pipenv run ipython . -i
    pipenv python -m flask run
}

# pause
# runpython
tmux new-session  'webpack ' ';' split-window 'pipenv run ipython __main__.py -i' ';'
# tmux new-session  'webpack ' ';' split-window 'export FLASK_APP=__main__:app && pipenv run python -m flask run || ipython -i' ';'
