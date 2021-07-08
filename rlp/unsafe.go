// Copyright 2021 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// +build !nacl,!js,cgo

package rlp

import (
	"math/big"
	"reflect"
	"unsafe"
)

// byteArrayBytes returns a slice of the byte array v.
func byteArrayBytes(v reflect.Value, length int) []byte {
	var s []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&s))
	hdr.Data = v.UnsafeAddr()
	hdr.Cap = length
	hdr.Len = length
	return s
}

// vref is a reference to a value of any type.
type vref struct {
	p unsafe.Pointer
}

// makevref creates a vref of v.
func makevref(tmp *vref, v interface{}) (vref, reflect.Type) {
	typ := reflect.TypeOf(v)
	eface := (*emptyInterface)(unsafe.Pointer(&v))

	var r vref
	if eface.typ.kind&kindDirectIface == 0 {
		// eface.word is '*type'
		r.p = eface.word
	} else {
		// eface.word is not '*type', it's just 'type'. This path is taken for values that
		// are pointers themselves, as well as one-field structs or one-elem arrays
		// containing a pointer.
		tmp.p = eface.word
		r.p = unsafe.Pointer(&tmp.p)
	}
	return r, typ
}

// sliceLen returns the length of r as a slice.
func (r vref) sliceLen() int {
	return (*reflect.SliceHeader)(r.p).Len
}

// sliceElem returns a reference to the i'th element of a slice.
func (r vref) sliceElem(esize uintptr, i int) vref {
	data := unsafe.Pointer((*reflect.SliceHeader)(r.p).Data)
	return vref{unsafe.Pointer(uintptr(data) + uintptr(esize*uintptr(i)))}
}

// arrayElem returns a reference to the i'th element of an array.
func (r vref) arrayElem(esize uintptr, i int) vref {
	return vref{unsafe.Pointer(uintptr(r.p) + uintptr(esize*uintptr(i)))}
}

// isZero returns true when r is the zero value of typ.
func (r vref) isZero(typ reflect.Type) bool {
	// XXX: this is probably too sloppy.
	// Package reflect does a type-specific check for this.
	for _, b := range r.byteArrayBytes(int(typ.Size())) {
		if b != 0 {
			return false
		}
	}
	return true
}

// byteArrayBytes returns r[:len:len].
func (r vref) byteArrayBytes(len int) []byte {
	var s []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&s))
	hdr.Data = uintptr(r.p)
	hdr.Cap = len
	hdr.Len = len
	return s
}

// structField returns the field f of r as a struct.
func (r vref) structField(f *field) vref {
	fr := vref{unsafe.Pointer(uintptr(r.p) + f.offset)}
	return fr
}

// ptrIsNil returns true if r is a nil pointer.
func (r vref) ptrIsNil() bool {
	return *(*unsafe.Pointer)(r.p) == nil
}

// ptrElem dereferences r as a pointer.
func (r vref) ptrElem() vref {
	return vref{*(*unsafe.Pointer)(r.p)}
}

// primitive value conversions

func (r vref) bool() bool        { return *(*bool)(r.p) }
func (r vref) string() string    { return *(*string)(r.p) }
func (r vref) byteSlice() []byte { return *(*[]byte)(r.p) }
func (r vref) bigInt() *big.Int  { return (*big.Int)(r.p) }

func (r vref) iface(typ reflect.Type) interface{} {
	return reflect.NewAt(typ, r.p).Elem().Interface()
}

func (r vref) ifaceValue(typ reflect.Type) reflect.Value {
	return reflect.NewAt(typ, r.p)
}

func (r vref) uint64(kind reflect.Kind) uint64 {
	switch kind {
	case reflect.Uint:
		return uint64(*(*uint)(r.p))
	case reflect.Uint8:
		return uint64(*(*uint8)(r.p))
	case reflect.Uint16:
		return uint64(*(*uint16)(r.p))
	case reflect.Uint32:
		return uint64(*(*uint32)(r.p))
	case reflect.Uint64:
		return *(*uint64)(r.p)
	case reflect.Uintptr:
		return uint64(*(*uintptr)(r.p))
	}
	panic("bad kind")
}

// copies of runtime structs

type emptyInterface struct {
	typ  *rtype
	word unsafe.Pointer
}

type rtype struct {
	size       uintptr
	ptrdata    uintptr
	hash       uint32
	tflag      uint8
	align      uint8
	fieldAlign uint8
	kind       uint8
	// rtype contains more fields, but we don't care about
	// them here, we just need 'kind'.
}

// if this is set in rtype.kind, emptyInterface.word contains
// the actual value, not the pointer to the value.
const kindDirectIface = 1 << 5
