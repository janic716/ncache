package utils

import "encoding/json"

func JsonEncodeIndent(stru interface{}) string {
	json, err := json.MarshalIndent(&stru, "", " ")
	if err == nil {
		return string(json)
	}
	return ""
}

func JsonEncode(stru interface{}) string {
	json, err := json.Marshal(stru)
	if err == nil {
		return string(json)
	}
	return ""
}

func JsonDecode2Map(data []byte) (map[string]interface{}, error) {
	var res = make(map[string]interface{})
	if err := json.Unmarshal(data, &res); err != nil {
		return nil, err
	}
	return res, nil
}

func JsonDecode2Stru(data []byte, stru interface{}) error {
	if err := json.Unmarshal(data, &stru); err != nil {
		return err
	}
	return nil
}

func JsonDecode2Rawmap(data []byte) (map[string]*json.RawMessage, error) {
	var res = make(map[string]*json.RawMessage)
	if err := json.Unmarshal(data, &res); err != nil {
		return nil, err
	}
	return res, nil
}
