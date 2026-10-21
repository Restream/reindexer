//go:build amd64 && !purego

#include "textflag.h"

#define DECODE16(in, out) \
	MOVO in, X10; \
	MOVO in, X11; \
	PSRLW $4, X10; \
	PAND X5, X10; \
	PAND X5, X11; \
	MOVO X6, X12; \
	PSHUFB X10, X12; \
	MOVO X11, X13; \
	PCMPGTB X12, X13; \
	MOVO X7, X12; \
	PSHUFB X10, X12; \
	MOVO X12, X14; \
	PCMPGTB X11, X14; \
	PAND X14, X13; \
	PMOVMSKB X13, AX; \
	CMPL AX, $0xffff; \
	JNE fail; \
	MOVO X8, X12; \
	PSHUFB X10, X12; \
	PADDB X12, X11; \
	MOVO X11, X12; \
	PSLLW $4, X12; \
	PSRLW $8, X11; \
	POR X11, X12; \
	PSHUFB X9, X12; \
	MOVQ X12, out; \
	BSWAPQ out

// func parseUuidFastAsm(str string) (hi uint64, lo uint64, ok bool)
TEXT ·parseUuidFastAsm(SB), NOSPLIT, $0-40
	MOVQ str_base+0(FP), SI
	MOVQ str_len+8(FP), CX
	MOVOU ·uuidHexLowMask<>(SB), X5
	MOVOU ·uuidHexMinMinusOne<>(SB), X6
	MOVOU ·uuidHexMaxPlusOne<>(SB), X7
	MOVOU ·uuidHexOffset<>(SB), X8
	MOVOU ·uuidHexPack<>(SB), X9

	CMPQ CX, $32
	JE compact
	CMPQ CX, $36
	JE dashed
	JMP fail

compact:
	MOVOU 0(SI), X0
	DECODE16(X0, R8)
	MOVOU 16(SI), X0
	DECODE16(X0, R9)
	JMP validate

dashed:
	CMPB 8(SI), $'-'
	JNE fail
	CMPB 13(SI), $'-'
	JNE fail
	CMPB 18(SI), $'-'
	JNE fail
	CMPB 23(SI), $'-'
	JNE fail

	MOVOU 0(SI), X0
	MOVOU 16(SI), X1
	MOVOU ·uuidDashFirstA<>(SB), X2
	PSHUFB X2, X0
	MOVOU ·uuidDashFirstB<>(SB), X2
	PSHUFB X2, X1
	POR X1, X0
	DECODE16(X0, R8)

	MOVOU 19(SI), X0
	MOVOU ·uuidDashSecondA<>(SB), X2
	PSHUFB X2, X0
	MOVBLZX 35(SI), AX
	MOVD AX, X1
	PSLLDQ $15, X1
	POR X1, X0
	DECODE16(X0, R9)

validate:
	MOVQ R8, AX
	ORQ R9, AX
	JZ ok
	TESTQ R9, R9
	JGE fail

ok:
	MOVQ R8, hi+16(FP)
	MOVQ R9, lo+24(FP)
	MOVB $1, ok+32(FP)
	RET

fail:
	MOVQ $0, hi+16(FP)
	MOVQ $0, lo+24(FP)
	MOVB $0, ok+32(FP)
	RET

DATA ·uuidHexLowMask<>+0(SB)/8, $0x0f0f0f0f0f0f0f0f
DATA ·uuidHexLowMask<>+8(SB)/8, $0x0f0f0f0f0f0f0f0f
GLOBL ·uuidHexLowMask<>(SB), RODATA|NOPTR, $16

DATA ·uuidHexOffset<>+0(SB)/8, $0x0009000900000000
DATA ·uuidHexOffset<>+8(SB)/8, $0x0000000000000000
GLOBL ·uuidHexOffset<>(SB), RODATA|NOPTR, $16

DATA ·uuidHexMinMinusOne<>+0(SB)/8, $0x7f007f00ff7f7f7f
DATA ·uuidHexMinMinusOne<>+8(SB)/8, $0x7f7f7f7f7f7f7f7f
GLOBL ·uuidHexMinMinusOne<>(SB), RODATA|NOPTR, $16

DATA ·uuidHexMaxPlusOne<>+0(SB)/8, $0x000700070a000000
DATA ·uuidHexMaxPlusOne<>+8(SB)/8, $0x0000000000000000
GLOBL ·uuidHexMaxPlusOne<>(SB), RODATA|NOPTR, $16

DATA ·uuidHexPack<>+0(SB)/8, $0x0e0c0a0806040200
DATA ·uuidHexPack<>+8(SB)/8, $0x8080808080808080
GLOBL ·uuidHexPack<>(SB), RODATA|NOPTR, $16

DATA ·uuidDashFirstA<>+0(SB)/8, $0x0706050403020100
DATA ·uuidDashFirstA<>+8(SB)/8, $0x80800f0e0c0b0a09
GLOBL ·uuidDashFirstA<>(SB), RODATA|NOPTR, $16

DATA ·uuidDashFirstB<>+0(SB)/8, $0x8080808080808080
DATA ·uuidDashFirstB<>+8(SB)/8, $0x0100808080808080
GLOBL ·uuidDashFirstB<>(SB), RODATA|NOPTR, $16

DATA ·uuidDashSecondA<>+0(SB)/8, $0x0807060503020100
DATA ·uuidDashSecondA<>+8(SB)/8, $0x800f0e0d0c0b0a09
GLOBL ·uuidDashSecondA<>(SB), RODATA|NOPTR, $16
