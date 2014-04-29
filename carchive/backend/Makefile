# to regenerate from the .proto as needed

PROTOC:=protoc

all: EPICSEvent_pb2.py

clean:
	rm -f EPICSEvent_pb2.py*

%_pb2.py: %.proto
	$(PROTOC) --python_out=$(PWD) $<
