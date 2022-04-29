// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package message

import (
	"encoding/binary"
	"math"

	"github.com/dolthub/dolt/go/store/hash"
)

const (
	maxChunkSz  = math.MaxUint16
	addrSz      = hash.ByteLen
	offsetCount = maxChunkSz / addrSz
	uint16Size  = 2
)

var addressOffsets []byte

func init() {
	addressOffsets = make([]byte, offsetCount*uint16Size)

	buf := addressOffsets
	off := uint16(addrSz)
	for len(buf) > 0 {
		binary.LittleEndian.PutUint16(buf, off)
		buf = buf[uint16Size:]
		off += uint16(addrSz)
	}
}

func offsetsForAddressArray(arr []byte) (offs []byte) {
	cnt := len(arr) / addrSz
	offs = addressOffsets[:cnt*uint16Size]
	return
}
