# Run this shell script from the pb directory to regenerate the PB wrapper.

protoc --python_out=. EPICSEvent.proto
