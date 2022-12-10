SRC=$(wildcard *.c)
OBJS=$(patsubst %.c, %.o, $(SRC))
TARGET=test
CC=gcc

#$@: 表示目标
#$<: 表示第一个依赖
#$^: 表示所有的依赖
#wildcard – 查找指定目录下的指定类型的文件
#src = $(wildcard *.c) //找到当前目录下所有后缀为.c的文件,赋值给src
#patsubst – 匹配替换
#obj = $(patsubst %.c,%.o, $(src)) //把src变量里所有后缀为.c的文件替换成.o

$(TARGET):$(OBJS)
	$(CC) $(OBJS) -o $(TARGET)

%.o:%.c
	$(CC) -c $< -o $@

clean:
	rm -rf $(OBJS) $(TARGET)
