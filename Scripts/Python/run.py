# -*- coding: utf-8 -*-
# author: nicktrejo

from dash_demo.app import app

if __name__ == '__main__':
    app.run_server(debug=True, dev_tools_hot_reload=True,
                   dev_tools_hot_reload_interval=10,
                   dev_tools_hot_reload_watch_interval=3,
                   host='0.0.0.0',
                  )
else:
    app.run_server(debug=False)
