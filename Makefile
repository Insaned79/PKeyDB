APP = PKeyDB
SRC = main.pas

FPCOPTS = @fpc_opts.txt

all: $(APP)

$(APP): $(SRC)
	fpc $(FPCOPTS) $(SRC)

clean:
	rm -f *.o *.ppu $(APP) 