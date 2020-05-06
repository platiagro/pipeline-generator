#!/bin/bash

content=$(cat output.ipynb)

curl -v -X PUT http://server.anonymous:80/notebook/anonymous/server/api/contents/experiments -H 'Content-Type: application/json' -H 'X-XSRFToken: token' -H 'Cookie: _xsrf=token' --data '{"type":"directory"}'
curl -v -X PUT http://server.anonymous:80/notebook/anonymous/server/api/contents/experiments/$1 -H 'Content-Type: application/json' -H 'X-XSRFToken: token' -H 'Cookie: _xsrf=token' --data '{"type":"directory"}'
curl -v -X PUT http://server.anonymous:80/notebook/anonymous/server/api/contents/experiments/$1/operators -H 'Content-Type: application/json' -H 'X-XSRFToken: token' -H 'Cookie: _xsrf=token' --data '{"type":"directory"}'
curl -v -X PUT http://server.anonymous:80/notebook/anonymous/server/api/contents/experiments/$1/operators/$2 -H 'Content-Type: application/json' -H 'X-XSRFToken: token' -H 'Cookie: _xsrf=token' --data '{"type":"directory"}'
curl -v -X PUT http://server.anonymous:80/notebook/anonymous/server/api/contents/experiments/$1/operators/$2/Inference.ipynb 'Content-Type: application/json' -H 'X-XSRFToken: token' -H 'Cookie: _xsrf=token' --data "{\"type\":\"notebook\", \"content\":$content}"
