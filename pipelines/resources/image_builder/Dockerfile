FROM platiagro/platiagro-notebook-image:0.1.0

WORKDIR /app

ENV MODEL_NAME Model
ENV API_TYPE REST
ENV SERVICE_TYPE MODEL
ENV PERSISTENCE 0
ENV PARAMETERS '[{"type":"STRING","name":"dataset","value":""}]'
ENV LOG_LEVEL DEBUG

EXPOSE 5000

# COPY *.py /app/

CMD ["sh", "-c", "seldon-core-microservice \"$MODEL_NAME\" \"$API_TYPE\" --service-type \"$SERVICE_TYPE\" --persistence \"$PERSISTENCE\" --parameters \"$PARAMETERS\" --log-level \"$LOG_LEVEL\""]
