FROM golang:1.8.1-alpine

ADD goddd /
ADD booking/docs /booking/docs
ADD booking/icons /booking/icons
ADD tracking/docs /tracking/docs
ADD handling/docs /handling/docs
EXPOSE 8080
CMD ["/goddd"]
