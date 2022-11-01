package main

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func schoolData(city string, num uint32, addr string) types.Value {
	return types.StructValue(
		types.StructFieldValue("city", types.TextValue(city)),
		types.StructFieldValue("number", types.Uint32Value(num)),
		types.StructFieldValue("address", types.TextValue(addr)),
	)
}

func getSchoolData() types.Value {
	return types.ListValue(
		schoolData("Орлов", 1, "Ст.Халтурина, 2"),
		schoolData("Орлов", 2, "Свободы, 4"),
		schoolData("Яранск", 1, "Гоголя, 25"),
		schoolData("Яранск", 2, "Кирова, 18"),
		schoolData("Яранск", 3, "Некрасова, 59"),
		schoolData("Кирс", 3, "Кирова, 6"),
		schoolData("Нолинск", 1, "Коммуны, 4"),
		schoolData("Нолинск", 2, "Федосеева, 2Б"),
		schoolData("Котельнич", 1, "Урицкого, 21"),
		schoolData("Котельнич", 2, "Октябрьская, 109"),
		schoolData("Котельнич", 3, "Советская, 153"),
		schoolData("Котельнич", 5, "Школьная, 2"),
		schoolData("Котельнич", 15, "Октябрьская, 91"),
	)
}
