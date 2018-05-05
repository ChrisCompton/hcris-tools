package hcris-tools

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

var AppConfig Config

type Config struct {
	Source struct {
		Download  bool   `yaml:"download"`
		Directory string `yaml:"directory"`
		FixedDate string `yaml:"fixedDate"`
		Url       string `yaml:"url"`
		Path      string `yaml:"path"`
		List      string `yaml:"list"`
		TagAttr   string `yaml:"tagAttr"`
		TagMatch  string `yaml:"tagMatch"`
		Host      string `yaml:"host"`
		Password  string `yaml:"password"`
	} `yaml:"source"`
	Store struct {
		Extract   bool   `yaml:"extract"`
		Directory string `yaml:"directory"`
		Prefix    string `yaml:"prefix"`
		Ext       string `yaml:"ext"`
		File      string `yaml:"file"`
		MaxQueue  int    `yaml:"maxqueue"`
	} `yaml:"store"`
	Settings struct {
		Debug   bool   `yaml:"debug"`
		Verbose bool   `yaml:"verbose"`
		Output  string `yaml:"output"`
		Logfile string `yaml:"logfile"`
	} `yaml:"settings"`
}

func LoadConfig(file string) error {
	configFile, err := ioutil.ReadFile(file)
	Check(err)

	Check(yaml.Unmarshal(configFile, &AppConfig))

	return err
}
