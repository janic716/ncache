package utils

import (
	"testing"
)

var Slice = []string{
	"Test",
	"String",
	"In",
	"Slice",
}

var NotInSlice = []string{
	"NoTest",
	"NoString",
	"NoIn",
	"NoSlice",
}

func TestStringInSlice(t *testing.T) {
	for index, v := range Slice {
		AssertMust(index == StringInSlice(v, Slice))
	}

	for _, v := range NotInSlice {
		AssertMust(-1 == StringInSlice(v, Slice))
	}
}

var PreStringSlice = []string{
	"prefixA",
	"prefixBTest",
	"prefixCTest",
	"prefixDTest",
}

var prefixSlice = []string{
	"prefixA",
	"prefixB",
	"prefixC",
	"prefixD",
}

func TestHasPrefixWithInSlice(t *testing.T) {
	for index, pre := range prefixSlice {
		AssertMust(index == HasPrefixWithInSlice(pre, PreStringSlice))
	}

	noExistPre := "prefixUn"
	AssertMust(-1 == HasPrefixWithInSlice(noExistPre, PreStringSlice))
}
