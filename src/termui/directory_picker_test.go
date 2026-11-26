package termui

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDirectoryTreeNavigation(t *testing.T) {
	root := t.TempDir()
	alpha := filepath.Join(root, "alpha")
	nested := filepath.Join(alpha, "nested")
	beta := filepath.Join(root, "beta")
	for _, directory := range []string{nested, beta} {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	tree, err := newDirectoryTree(root)
	if err != nil {
		t.Fatal(err)
	}
	nodes := tree.visible()
	if len(nodes) != 3 || nodes[1].path != alpha || nodes[2].path != beta {
		t.Fatalf("visible folders = %#v; want root, alpha, beta", nodes)
	}
	tree.move(1)
	if tree.selected.path != alpha {
		t.Fatalf("first down selected %q; want %q", tree.selected.path, alpha)
	}
	if err := tree.expand(tree.selected); err != nil {
		t.Fatal(err)
	}
	tree.move(1)
	if tree.selected.path != nested {
		t.Fatalf("down after expanding selected %q; want %q", tree.selected.path, nested)
	}
	tree.selected.expanded = false
	tree.move(-1)
	if tree.selected.path != alpha {
		t.Fatalf("up after collapsing selected %q; want %q", tree.selected.path, alpha)
	}
}

func TestDirectoryTreeMovesToParent(t *testing.T) {
	parent := t.TempDir()
	start := filepath.Join(parent, "start")
	if err := os.Mkdir(start, 0o755); err != nil {
		t.Fatal(err)
	}
	tree, err := newDirectoryTree(start)
	if err != nil {
		t.Fatal(err)
	}
	tree.selected.expanded = false
	if err := tree.moveToParent(); err != nil {
		t.Fatal(err)
	}
	if tree.root.path != parent || tree.selected.path != start {
		t.Fatalf("root/selection = (%q, %q); want (%q, %q)", tree.root.path, tree.selected.path, parent, start)
	}
}

func TestDirectoryPickerStartsAtFilesystemVolumeRoot(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	want := string(os.PathSeparator)
	if volume := filepath.VolumeName(home); volume != "" {
		want = volume + string(os.PathSeparator)
	}
	if got := directoryPickerRootPath(home); got != want {
		t.Fatalf("picker root = %q; want %q", got, want)
	}
}

func TestReadDirectoryPickerArrowKeys(t *testing.T) {
	for _, test := range []struct {
		name string
		keys string
		want directoryPickerKey
	}{
		{name: "up", keys: "\x1b[A", want: directoryPickerUp},
		{name: "down", keys: "\x1bOB", want: directoryPickerDown},
		{name: "left", keys: "\x00K", want: directoryPickerLeft},
		{name: "right", keys: "\xe0M", want: directoryPickerRight},
		{name: "enter", keys: "\r", want: directoryPickerEnter},
		{name: "cancel", keys: "q", want: directoryPickerCancel},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := readDirectoryPickerKey(bytes.NewBufferString(test.keys))
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("key = %d; want %d", got, test.want)
			}
		})
	}
}

func TestRunDirectoryPickerSelectsAndCancels(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	tree, err := newDirectoryTree(root)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	selected, err := runDirectoryPicker(bytes.NewBufferString("\x1b[B\r"), &output, tree, 24)
	if err != nil {
		t.Fatal(err)
	}
	if selected != target || !bytes.Contains(output.Bytes(), []byte("└── [+] target")) {
		t.Fatalf("selected = %q, rendered tree = %q; want %q and a branch for target", selected, output.String(), target)
	}

	_, err = runDirectoryPicker(bytes.NewBufferString("q"), &bytes.Buffer{}, tree, 24)
	if !errors.Is(err, ErrDirectorySelectionCancelled) {
		t.Fatalf("cancel error = %v; want cancellation", err)
	}
}
