package storage

import (
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/hypergnomon/hypergnomon/structures"
)

const ntxTestScid = "a05395bb0cf77adc850928b0db00eb5ca7a9ccbafd9a38d021c8d299ad5ce1a4"

// TestNormalTx_TypedRoundTrip pins the tag-0x08 encoder: full struct equality
// (all four fields are comparable), tag detection, append/marshal parity, the
// truncated-buffer guard, and msgpack rejection by the typed decoder.
func TestNormalTx_TypedRoundTrip(t *testing.T) {
	cases := []structures.NormalTXWithSCIDParse{
		{},
		{Txid: "aa", Scid: ntxTestScid, Fees: 1, Height: 10},
		{
			Txid:   "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
			Scid:   ntxTestScid,
			Fees:   18446744073709551615,
			Height: -1,
		},
	}
	for _, c := range cases {
		b := c.MarshalTyped()
		if !structures.IsNormalTxTyped(b) {
			t.Fatalf("typed normaltx not detected: %x", b)
		}
		if b[0] != 0x08 {
			t.Fatalf("typed tag = 0x%02x, want 0x08", b[0])
		}
		var got structures.NormalTXWithSCIDParse
		if err := got.UnmarshalTyped(b); err != nil {
			t.Fatalf("UnmarshalTyped(%+v): %v", c, err)
		}
		if got != c {
			t.Fatalf("round-trip drift: got %+v want %+v", got, c)
		}
		if ab := c.MarshalTypedAppend(nil); string(ab) != string(b) {
			t.Fatalf("MarshalTypedAppend != MarshalTyped:\n  %x\n  %x", ab, b)
		}
	}

	// Truncated buffer must error, not panic.
	full := cases[1].MarshalTyped()
	for trunc := 0; trunc < len(full); trunc++ {
		var got structures.NormalTXWithSCIDParse
		if err := got.UnmarshalTyped(full[:trunc]); err == nil {
			t.Fatalf("UnmarshalTyped accepted truncated len=%d", trunc)
		}
	}

	// msgpack bytes must not be mis-identified as typed.
	mp, err := msgpack.Marshal(&cases[1])
	if err != nil {
		t.Fatal(err)
	}
	if structures.IsNormalTxTyped(mp) {
		t.Fatalf("msgpack mis-identified as typed: first byte 0x%02x", mp[0])
	}
}

// TestDecodeNormalTxEntry_ThreeWayDispatch drives the three on-disk shapes the
// reader must distinguish, plus the empty-value guard.
func TestDecodeNormalTxEntry_ThreeWayDispatch(t *testing.T) {
	addr := "dero1qyjjxxaabbccddeeff0011223344556677889900aabbccddee00112233445566"

	// (i) typed value under a composite key — what FlushBatch now writes.
	typedRec := structures.NormalTXWithSCIDParse{Txid: "bb", Scid: ntxTestScid, Fees: 7, Height: 20}
	ck := appendNormTxKey(nil, addr, typedRec.Height, typedRec.Txid, typedRec.Scid)
	gotAddr, recs, err := DecodeNormalTxEntry(ck, typedRec.MarshalTyped())
	if err != nil || gotAddr != addr || len(recs) != 1 {
		t.Fatalf("typed dispatch: addr=%q recs=%d err=%v", gotAddr, len(recs), err)
	}
	if *recs[0] != typedRec {
		t.Fatalf("typed fields lost: got %+v want %+v", *recs[0], typedRec)
	}

	// (ii) legacy single-record msgpack under a composite key — byte-identical
	// to on-disk records written by the pre-typed FlushBatch AND by
	// StoreNormalTxWithSCIDByAddr (which intentionally still emits msgpack).
	legRec := structures.NormalTXWithSCIDParse{Txid: "dd", Scid: ntxTestScid, Fees: 4, Height: 40}
	mp, err := msgpack.Marshal(&legRec)
	if err != nil {
		t.Fatal(err)
	}
	ck2 := appendNormTxKey(nil, addr, legRec.Height, legRec.Txid, legRec.Scid)
	_, recs, err = DecodeNormalTxEntry(ck2, mp)
	if err != nil || len(recs) != 1 || *recs[0] != legRec {
		t.Fatalf("legacy single-record msgpack dispatch: recs=%+v err=%v", recs, err)
	}

	// (iii) legacy []txs blob under a bare addr key.
	blob, err := msgpack.Marshal([]*structures.NormalTXWithSCIDParse{&legRec})
	if err != nil {
		t.Fatal(err)
	}
	_, recs, err = DecodeNormalTxEntry([]byte(addr), blob)
	if err != nil || len(recs) != 1 || recs[0].Txid != "dd" {
		t.Fatalf("legacy []txs blob dispatch: recs=%+v err=%v", recs, err)
	}

	// (c) empty value under a composite key must error, not panic.
	if _, _, err := DecodeNormalTxEntry(ck, nil); err == nil {
		t.Fatal("empty composite value must error, not decode")
	}
	if _, _, err := DecodeNormalTxEntry(ck, []byte{}); err == nil {
		t.Fatal("empty composite value must error, not decode")
	}
}
