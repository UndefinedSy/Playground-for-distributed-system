test: consistent.o test.o
	clang++ -o test consistent.o test.o
test.o:
	clang++ -c test.cpp
consistent.o:
	clang++ -c consistent.cpp

clean:
	rm *.o