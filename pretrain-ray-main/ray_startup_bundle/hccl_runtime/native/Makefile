ASCEND_DIR ?= /usr/local/Ascend/cann/ascend-toolkit/latest
CXX ?= g++

TARGET := bin/ranktable_allreduce_probe
SOURCE := src/ranktable_allreduce_probe.cc

CXXFLAGS += -std=c++11 -O2 -Wall -Wextra -Wpedantic -Werror \
	-fstack-protector-strong -fPIE -MMD -MP
CPPFLAGS += -isystem $(ASCEND_DIR)/include
LDFLAGS += -pie -Wl,-z,relro -Wl,-z,now -Wl,-z,noexecstack \
	-L$(ASCEND_DIR)/lib64 -Wl,-rpath,$(ASCEND_DIR)/lib64
LDLIBS += -lhccl -lascendcl

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(SOURCE) Makefile
	@mkdir -p bin
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS)

clean:
	rm -f $(TARGET) $(TARGET).d

-include $(TARGET).d
