package main

type BitSets []int64

func NewBitSets(n uint) BitSets {
	bs := make(BitSets, n/64+1)
	return bs
}

func (bs BitSets) Set(index uint) {
	index, bit := index/64, index%64
	bs[index] |= 1 << bit
}

func (bs BitSets) Unset(index uint) {
	index, bit := index/64, index%64
	bs[index] ^= 1 << bit
}

func (bs BitSets) IsSet(index uint) bool {
	index, bit := index/64, index%64
	word := bs[index]
	return (word | (1 << bit)) == word
}

/*import (
"bytes"
)

//bitSet实现
type BitSet []uint64

const (
	Address_Bits_Per_Word uint8  = 6
	Words_Per_Size        uint64 = 64 //单字64位
)

//创建指定初始化大小的bitSet
func NewBitMap(nbits int) *BitSet {
	wordsLen := (nbits - 1) >> Address_Bits_Per_Word
	temp := BitSet(make([]uint64, wordsLen+1, wordsLen+1))
	return &temp
}

//把指定位置设为ture
func (this *BitSet) Set(bitIndex uint64) {
	wIndex := this.wordIndex(bitIndex)
	this.expandTo(wIndex)
	(*this)[wIndex] |= uint64(0x01) << (bitIndex % Words_Per_Size)
}

//设置指定位置为false
func (this *BitSet) Clear(bitIndex uint64) {
	wIndex := this.wordIndex(bitIndex)
	if wIndex < len(*this) {
		(*this)[wIndex] &^= uint64(0x01) << (bitIndex % Words_Per_Size)
	}
}

//获取指定位置的值
func (this *BitSet) Get(bitIndex uint64) bool {
	wIndex := this.wordIndex(bitIndex)
	return (wIndex < len(*this)) && ((*this)[wIndex]&(uint64(0x01)<<(bitIndex%Words_Per_Size)) != 0)
}

//以二进制串的格式打印bitMap内容
func (this *BitSet) ToString() string {
	var temp uint64
	strAppend := &bytes.Buffer{}
	for i := 0; i < len(*this); i++ {
		temp = (*this)[i]
		for j := 0; j < 64; j++ {
			if temp&(uint64(0x01)<<uint64(j)) != 0 {
				strAppend.WriteString("1")
			} else {
				strAppend.WriteString("0")
			}
		}
	}
	return strAppend.String()
}

//定位位置
func (this BitSet) wordIndex(bitIndex uint64) int {
	return int(bitIndex >> Address_Bits_Per_Word)
}

//动态扩容:每次扩容两倍
func (this *BitSet) expandTo(wordIndex int) {
	wordsRequired := wordIndex + 1
	if len(*this) < wordsRequired {
		if wordsRequired < 2*len(*this) {
			wordsRequired = 2 * len(*this)
		}
		newCap := make([]uint64, wordsRequired, wordsRequired)
		copy(newCap, *this)
		(*this) = newCap
	}
}*/

