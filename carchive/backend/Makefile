# to regenerate from the .proto as needed

PROTOC:=protoc

all: EPICSEvent_pb2.py EPICSEvent_pb2.h

clean:
	rm -f EPICSEvent_pb2.py*
	rm -f EPICSEvent.pb.h EPICSEvent.pb.cc

%_pb2.py: %.proto
	$(PROTOC) --python_out=$$PWD $<

%_pb2.h: %.proto
	$(PROTOC) --cpp_out=$$PWD $<
