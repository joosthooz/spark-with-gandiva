

int main(int argc, char* argv[]) {

//define i32 @expr_0_0(i64* nocapture readonly %inputs_addr, i64* nocapture readonly %inputs_addr_offsets, i64* nocapture readnone %local_bitmaps, i16* nocapture readnone %selection_vector, i64 %context_ptr, i64 %nrecords)

	long *inputs_addr = 0x10000;
	long *inputs_addr_offsets = 0;
	long *local_bitmaps = 0;
	short *selection_vector = 0;
	long context_ptr = 0;
	long nrecords = 1e6;

	expr_0_0(inputs_addr, inputs_addr_offsets, local_bitmaps, selection_vector, context_ptr, nrecords);
//	expr_0_0(inputs_addr);
//	expr_0_0();
	return 0;
}
