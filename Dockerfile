FROM node:12.3.1-slim

WORKDIR /opt/app
COPY . .

RUN npm install

ENTRYPOINT [ "npm", "start" ]
