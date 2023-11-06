# Image for serving NGINX with all its static
# All right reserved PT. SUMBER MAKMUR SURYA PERKASA

FROM nginx:1.22.1-alpine

COPY ./ssl-smsperkasa.co.id.conf /etc/nginx/snippets/ssl-smsperkasa.co.id.conf
COPY ./ssl-smsperkasa.com.conf /etc/nginx/snippets/ssl-smsperkasa.com.conf
COPY ./ssl-params.conf /etc/nginx/snippets/ssl-params.conf
COPY ./conf /etc/conf

COPY ./nginx-app.conf /etc/nginx/conf.d/default.conf
COPY ./nginx.conf /etc/nginx/nginx.conf
