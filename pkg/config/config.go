// Package config fills a yaml tagged struct from a yaml file, env vars and command line flags.
package config

import (
	"encoding"
	"flag"
	"fmt"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"unicode"

	"go.yaml.in/yaml/v3"
)

type field struct {
	path string // dotted yaml path, also the flag name
	env  string
	v    reflect.Value
}

type assignment struct {
	field field
	text  string
}

// fileValue is one scalar from the config file.
type fileValue struct {
	path string
	text string
	line int
}

// Load fills dst, a pointer to a yaml tagged struct, from the -config yaml file, then env vars, then flags in fs.
// each leaf field gets a flag named by its dotted yaml path and an env var PREFIX_PATH_IN_SNAKE_CASE.
// integer fields accept a B, KiB, MiB, GiB or TiB suffix from every source.
func Load(dst any, envPrefix string, fs *flag.FlagSet, args []string) error {
	configEnv := envPrefix + "_CONFIG"
	configPath := fs.String("config", os.Getenv(configEnv), "yaml config file, env "+configEnv)

	fields := collect(reflect.ValueOf(dst).Elem(), "", nil)
	var assigned []assignment
	for i := range fields {
		f := &fields[i]
		f.env = envName(envPrefix, f.path)
		usage := fmt.Sprintf("default %v, env %s", f.v.Interface(), f.env)
		fs.Func(f.path, usage, func(text string) error {
			// parse into a scratch value so a bad flag fails during Parse
			if err := setText(reflect.New(f.v.Type()).Elem(), text); err != nil {
				return err
			}
			assigned = append(assigned, assignment{*f, text})
			return nil
		})
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *configPath != "" {
		values, err := readFile(*configPath)
		if err != nil {
			return err
		}
		byPath := make(map[string]reflect.Value, len(fields))
		for _, f := range fields {
			byPath[f.path] = f.v
		}
		for _, fv := range values {
			v, ok := byPath[fv.path]
			if !ok {
				return fmt.Errorf("config %s: line %d: unknown key %s", *configPath, fv.line, fv.path)
			}
			if err := setText(v, fv.text); err != nil {
				return fmt.Errorf("config %s: line %d: %s: %w", *configPath, fv.line, fv.path, err)
			}
		}
	}
	for _, f := range fields {
		if text, ok := os.LookupEnv(f.env); ok {
			if err := setText(f.v, text); err != nil {
				return fmt.Errorf("config: env %s: %w", f.env, err)
			}
		}
	}
	for _, a := range assigned {
		if err := setText(a.field.v, a.text); err != nil {
			return fmt.Errorf("config: flag -%s: %w", a.field.path, err)
		}
	}
	return nil
}

// readFile flattens the yaml mapping at path into dotted scalar values.
func readFile(path string) ([]fileValue, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	if len(doc.Content) == 0 {
		return nil, nil
	}
	values, err := flatten(doc.Content[0], "", nil)
	if err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	return values, nil
}

func flatten(n *yaml.Node, prefix string, out []fileValue) ([]fileValue, error) {
	if n.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("line %d: expected a mapping", n.Line)
	}
	for i := 0; i < len(n.Content); i += 2 {
		key, val := n.Content[i], n.Content[i+1]
		path := key.Value
		if prefix != "" {
			path = prefix + "." + key.Value
		}
		switch val.Kind {
		case yaml.MappingNode:
			var err error
			if out, err = flatten(val, path, out); err != nil {
				return nil, err
			}
		case yaml.ScalarNode:
			out = append(out, fileValue{path: path, text: val.Value, line: val.Line})
		default:
			return nil, fmt.Errorf("line %d: %s must be a value or a mapping", val.Line, path)
		}
	}
	return out, nil
}

// collect lists the settable leaves of v, following yaml tag names and inlining.
func collect(v reflect.Value, prefix string, out []field) []field {
	t := v.Type()
	for i := range t.NumField() {
		sf := t.Field(i)
		if !sf.IsExported() && !sf.Anonymous {
			continue
		}
		name, opts, _ := strings.Cut(sf.Tag.Get("yaml"), ",")
		if name == "-" {
			continue
		}
		fv := v.Field(i)
		if strings.Contains(opts, "inline") {
			out = collect(fv, prefix, out)
			continue
		}
		if name == "" {
			name = strings.ToLower(sf.Name) // the yaml default
		}
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}
		if fv.Kind() == reflect.Struct && !isText(fv) {
			out = collect(fv, path, out)
			continue
		}
		out = append(out, field{path: path, v: fv})
	}
	return out
}

func isText(v reflect.Value) bool {
	_, ok := v.Addr().Interface().(encoding.TextUnmarshaler)
	return ok
}

func setText(v reflect.Value, text string) error {
	if u, ok := v.Addr().Interface().(encoding.TextUnmarshaler); ok {
		return u.UnmarshalText([]byte(text))
	}
	switch v.Kind() {
	case reflect.String:
		v.SetString(text)
	case reflect.Bool:
		b, err := strconv.ParseBool(text)
		if err != nil {
			return err
		}
		v.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		num, shift := splitUnit(text)
		n, err := strconv.ParseInt(num, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid integer %q", text)
		}
		if n > math.MaxInt64>>shift || n < math.MinInt64>>shift || v.OverflowInt(n<<shift) {
			return fmt.Errorf("%q out of range", text)
		}
		v.SetInt(n << shift)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		num, shift := splitUnit(text)
		n, err := strconv.ParseUint(num, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid integer %q", text)
		}
		if n > math.MaxUint64>>shift || v.OverflowUint(n<<shift) {
			return fmt.Errorf("%q out of range", text)
		}
		v.SetUint(n << shift)
	case reflect.Float32, reflect.Float64:
		n, err := strconv.ParseFloat(text, v.Type().Bits())
		if err != nil {
			return err
		}
		v.SetFloat(n)
	default:
		return fmt.Errorf("unsupported kind %s", v.Kind())
	}
	return nil
}

var byteUnits = [...]struct {
	suffix string
	shift  uint
}{{"TiB", 40}, {"GiB", 30}, {"MiB", 20}, {"KiB", 10}, {"B", 0}}

// splitUnit strips a binary byte unit suffix and returns the shift it stands for.
func splitUnit(text string) (string, uint) {
	for _, unit := range byteUnits {
		if num, ok := strings.CutSuffix(text, unit.suffix); ok {
			return strings.TrimSpace(num), unit.shift
		}
	}
	return text, 0
}

// envName maps memtable.capacityBytes to PREFIX_MEMTABLE_CAPACITY_BYTES.
func envName(prefix, path string) string {
	var b strings.Builder
	b.WriteString(prefix)
	b.WriteByte('_')
	var prev rune
	for _, r := range path {
		switch {
		case r == '.':
			b.WriteByte('_')
		case unicode.IsUpper(r) && (unicode.IsLower(prev) || unicode.IsDigit(prev)):
			b.WriteByte('_')
			b.WriteRune(r)
		default:
			b.WriteRune(unicode.ToUpper(r))
		}
		prev = r
	}
	return b.String()
}
