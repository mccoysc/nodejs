const express = require("express");
const mqtt = require("mqtt");
const http = require("http");
const crypto = require("crypto");


const replyTimeout = process.env.REPLY_TIMEOUT || 30000;
const mqttEndpoint = process.env.MQ_ENDPOINT || "mqtt://test.mosquitto.org";
const mqttTopic = process.env.MQ_TOPIC || "MQ_ON_HTTTP_REQUEST";
var mqttClient = mqtt.connect(mqttEndpoint);
var mqttOk = false;

async function* streamAsyncIterable(stream) {
    if (!stream.getReader) {
        var shouldLoop = true;
        while (shouldLoop) {
            yield (new Promise(function (resolve, reject) {
                stream.on("data", (chunk) => {
                    resolve(chunk);
                });
                stream.on("end", () => {
                    shouldLoop = false;
                    resolve();
                });
                stream.on("error", (err) => {
                    shouldLoop = false;
                    reject(err);
                });
            }));
        }
        return;
    } else {
        const reader = stream.getReader();
        try {
            while (true) {
                const { done, value } = await reader.read();
                if (done) {
                    return;
                }
                yield value;
            }
        } finally {
            reader.releaseLock();
        }
    }
}

const app = express();
const reply_wait_queue = {};
mqttClient.on("message", async function (topic, payload) {
    (function delTopic() {
        mqttClient.unsubscribe(topic, function (err) {
            if (err) {
                setTimeout(delTopic, 500);
            }
        })
    })();
    const res = reply_wait_queue[topic];
    delete reply_wait_queue[topic];
    if (!res) return;
    try {
        payload = JSON.parse(Buffer.from(payload).toString());
    } catch (error) {
        res.statusCode = 500;
        return res.end("server internal error");
    }
    res.statusCode = payload.statusCode;
    res.headers = payload.httpHeaders;
    if (payload.httpBody) {
        return res.end(Buffer.from(payload.httpBody, payload.httpBodyEncodeType));
    }
    res.end();
});
app.use(async (req, res, next) => {
    if (!mqttOk) {
        res.statusCode = 500;
        return res.end("invalid server status");
    }
    const replyId = crypto.randomBytes(16).toString("hex");
    var msgPayload = { replyTopic: replyId, httpMethod: req.method, httpPath: req.path, httpHeaders: req.headers };
    if (req.method.toLocaleLowerCase() === "post") {
        var chunks = [];
        for await (var chunk of streamAsyncIterable(req)) {
            if(chunk?.byteLength) chunks.push(chunk);
        }
        if (chunks?.length) {
            msgPayload.httpBodyEncodeType = "base64";
            msgPayload.httpBody = Buffer.concat(chunks).toString("base64");
        }
    } else if (req.method.toLocaleLowerCase() !== "get") {
        res.statusCode = 400;
        return res.end("only support get,post")
    }
    setTimeout(function () {
        const res = reply_wait_queue[replyId];
        delete reply_wait_queue[replyId];
        (function delTopic() {
            mqttClient.unsubscribe(replyId, function (err) {
                if (err && err.message && err.message.indexOf("Connection closed") < 0) {
                    setTimeout(delTopic, 500);
                }
            })
        })();
        if (res) {
            res.statusCode = 500;
            res.end("generate response timeout");
        }
    }, replyTimeout)
    mqttClient.subscribe(replyId);
    reply_wait_queue[replyId] = res;
    return mqttClient.publish(mqttTopic, JSON.stringify(msgPayload));
})


const listenPort = process.env.PORT || process.env.SERVER_PORT || process.env.APP_PORT || 80;
http.createServer(app).listen(listenPort, async function () {
    console.log(`http server listen at port ${listenPort}`);
    try {
        mqttOk = await new Promise(function (resolver) {
            mqttClient.once("connect", function () {
                resolver(true);
            });
        });
        console.log("mqtt connect ok");
    } catch (error) {
        console.log("mqtt  connect error");
    }
});

//for alinyun serverlesss function
var getRawBody;
var fetch_func = (typeof fetch !== typeof undefined ? fetch : require("node-fetch"));
exports.handler = async (req, resp, context) => {
    getRawBody = getRawBody || require('raw-body');
    var body
    try {
        body = req.method.toLocaleLowerCase() !== "post" ? undefined : (await (new Promise((resolver, reject) => {
            getRawBody(req, (err, body) => {
                if (err) {
                    return reject(err);
                }
                resolver(body);
            });
        })));
    } catch (error) {
        resp.statusCode = 500;
        return resp.send("get http body error");
    }
    const fetchOpts = {
        method: req.method,
        headers: req.headers
    };
    if (body) {
        fetchOpts.body = body;
    }
    const httpRes = await fetch_func(`http://127.0.0.1:${listenPort}${req.path}`, fetchOpts);
    resp.statusCode = httpRes.status / 1
    for ([k, v] of (new Map(httpRes.headers))) {
        resp.setHeader(k, v)
    }
    const chunks = [];
    for await (var chunk of streamAsyncIterable(httpRes.body)) {
        chunks.push(chunk);
    }
    var resData = Buffer.concat(chunks);
    resp.send(resData);
}
