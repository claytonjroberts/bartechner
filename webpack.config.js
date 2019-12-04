var path = require('path');

module.exports = {
    mode: 'development',

    resolve: {
        alias: {
            'vue$': 'vue/dist/vue.esm.js',
            '@fortawesome/fontawesome-free$': '@fortawesome/fontawesome-free-solid/shakable.es.js'
        }
    },

    // context: path.resolve(__dirname, 'src'), entry: './static/src/index.js',
    entry: {
        "index": path.resolve(__dirname, 'src/scripts/index.js'),
        // "app_forecast_deration": path.resolve(
        //     __dirname,
        //     'static/src/app_forecast_deration.js'
        // )
    },
    output: {
        filename: '[name].js',
        // filename: (chunkData) => {     return chunkData.chunk.name === 'main' ?
        // '[name].js'         : '[name]/[name].js'; },
        path: path.resolve(__dirname, 'static/dist')
    },
    module: {
        rules: [{
                test: /\.scss$/,
                use: [
                    "style-loader", // creates style nodes from JS strings
                    "css-loader", // translates CSS into CommonJS
                    "sass-loader", // compiles Sass to CSS, using Node Sass by default
                    // "url-loader",,,,,,,,,,,,,,,
                ]
            }, {
                test: /\.m?js$/,
                exclude: /(node_modules|bower_components)/,
                use: {
                    loader: 'babel-loader',
                    options: {
                        presets: ['@babel/preset-env']
                    }
                }
            }, {
                test: /\.less$/,
                loader: "less-loader",
            }, {
                test: /\.woff2?(\?v=[0-9]\.[0-9]\.[0-9])?$/,
                // Limiting the size of the woff fonts breaks font-awesome ONLY for the extract
                // text plugin loader: "url?limit=10000"
                loader: "url-loader"
            }, {
                test: /\.(ttf|eot|svg)(\?[\s\S]+)?$/,
                loader: 'file-loader'
            },
            // {     test: /\.woff2?(\?v=[0-9]\.[0-9]\.[0-9])?$/,      Limiting the size of
            // the woff fonts breaks font-awesome ONLY for the extract text plugin loader:
            // "url?limit=10000"     loader: "url-loader" }, {     test:
            // /\.(ttf|eot|svg)(\?[\s\S]+)?$/,     loader: 'file-loader' },  {      test:
            // /\.woff2?(\?v=[0-9]\.[0-9]\.[0-9])?$/,       Limiting the size of the woff
            // fonts breaks font-awesome ONLY for the extract text plugin       loader:
            // "url?limit=10000"      loader: "url-loader"  }, {     test:
            // /\.(ttf|eot|svg)(\?[\s\S]+)?$/,     loader: 'file-loader' }, {     test:
            // /(eot|woff|woff2|ttf|svg|png|jpe?g|gif)(\?\S*)?$/,     loader: 'url-loader',
            // use: {          loader: 'url-loader',          options: {              name:
            // '[name].[ext]',              outputPath: 'fonts/'          }      }
            // },,,,,,,,,,,,,,,
        ]
    },
    watchOptions: {
        ignored: /node_modules/
    },
    watch: true,

    // resolve: {     alias: {         'vue$': 'vue/dist/vue.esm.js'     },
    // extensions: ['*', '.js', '.vue', '.json'] },,,,,,,,,,,,,,,
};