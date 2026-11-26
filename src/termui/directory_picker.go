package termui

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/mattn/go-colorable"
	"golang.org/x/term"
)

var ErrDirectorySelectionCancelled = errors.New("folder selection cancelled")

type directoryNode struct {
	path     string
	parent   *directoryNode
	children []*directoryNode
	loaded   bool
	expanded bool
}

type directoryTree struct {
	root     *directoryNode
	selected *directoryNode
}

func newDirectoryTree(rootPath string) (*directoryTree, error) {
	absolutePath, err := filepath.Abs(rootPath)
	if err != nil {
		return nil, fmt.Errorf("resolve folder %q: %w", rootPath, err)
	}
	info, err := os.Stat(absolutePath)
	if err != nil {
		return nil, fmt.Errorf("open folder %q: %w", absolutePath, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%q is not a folder", absolutePath)
	}
	root := &directoryNode{path: absolutePath}
	tree := &directoryTree{root: root, selected: root}
	if err := tree.expand(root); err != nil {
		return nil, err
	}
	return tree, nil
}

func (tree *directoryTree) expand(node *directoryNode) error {
	if !node.loaded {
		entries, err := os.ReadDir(node.path)
		if err != nil {
			return fmt.Errorf("read folder %q: %w", node.path, err)
		}
		for _, entry := range entries {
			if entry.IsDir() {
				node.children = append(node.children, &directoryNode{
					path:   filepath.Join(node.path, entry.Name()),
					parent: node,
				})
			}
		}
		node.loaded = true
	}
	node.expanded = true
	return nil
}

func (tree *directoryTree) visible() []*directoryNode {
	nodes := []*directoryNode{tree.root}
	var visit func(*directoryNode)
	visit = func(parent *directoryNode) {
		if !parent.expanded {
			return
		}
		for _, child := range parent.children {
			nodes = append(nodes, child)
			visit(child)
		}
	}
	visit(tree.root)
	return nodes
}

func (tree *directoryTree) move(delta int) {
	nodes := tree.visible()
	selected := 0
	for index, node := range nodes {
		if node == tree.selected {
			selected = index
			break
		}
	}
	tree.selected = nodes[max(0, min(len(nodes)-1, selected+delta))]
}

func (tree *directoryTree) moveToParent() error {
	parentPath := filepath.Dir(tree.root.path)
	if parentPath == tree.root.path {
		return nil
	}
	parent, err := newDirectoryTree(parentPath)
	if err != nil {
		return err
	}
	for index, child := range parent.root.children {
		if filepath.Clean(child.path) != filepath.Clean(tree.root.path) {
			continue
		}
		tree.root.parent = parent.root
		parent.root.children[index] = tree.root
		tree.root = parent.root
		tree.selected = parent.root.children[index]
		return nil
	}
	return fmt.Errorf("folder %q was not found in its parent", tree.root.path)
}

func directoryBranch(node, root *directoryNode) string {
	if node == root {
		return ""
	}
	var ancestors []*directoryNode
	for current := node; current != nil && current != root; current = current.parent {
		ancestors = append(ancestors, current)
	}
	if len(ancestors) == 0 || ancestors[len(ancestors)-1].parent != root {
		return ""
	}
	var branch strings.Builder
	for index := len(ancestors) - 1; index >= 0; index-- {
		current := ancestors[index]
		parent := current.parent
		last := len(parent.children) > 0 && parent.children[len(parent.children)-1] == current
		if index == 0 {
			if last {
				branch.WriteString("└── ")
			} else {
				branch.WriteString("├── ")
			}
		} else if last {
			branch.WriteString("    ")
		} else {
			branch.WriteString("│   ")
		}
	}
	return branch.String()
}

type directoryPickerKey uint8

const (
	directoryPickerEnter directoryPickerKey = iota
	directoryPickerUp
	directoryPickerDown
	directoryPickerLeft
	directoryPickerRight
	directoryPickerCancel
)

func readDirectoryPickerKey(input io.Reader) (directoryPickerKey, error) {
	var first [1]byte
	if _, err := io.ReadFull(input, first[:]); err != nil {
		return 0, err
	}
	switch first[0] {
	case '\r', '\n':
		return directoryPickerEnter, nil
	case 3, 'q', 'Q':
		return directoryPickerCancel, nil
	case 0, 0xe0:
		var scan [1]byte
		if _, err := io.ReadFull(input, scan[:]); err != nil {
			return 0, err
		}
		switch scan[0] {
		case 'H':
			return directoryPickerUp, nil
		case 'P':
			return directoryPickerDown, nil
		case 'K':
			return directoryPickerLeft, nil
		case 'M':
			return directoryPickerRight, nil
		}
	case 0x1b:
		var sequence [2]byte
		if _, err := io.ReadFull(input, sequence[:]); err != nil {
			return 0, err
		}
		if sequence[0] != '[' && sequence[0] != 'O' {
			return 0, nil
		}
		switch sequence[1] {
		case 'A':
			return directoryPickerUp, nil
		case 'B':
			return directoryPickerDown, nil
		case 'D':
			return directoryPickerLeft, nil
		case 'C':
			return directoryPickerRight, nil
		}
	}
	return 0, nil
}

func renderDirectoryTree(output io.Writer, tree *directoryTree, rows int, status string) error {
	nodes := tree.visible()
	selected := 0
	for index, node := range nodes {
		if node == tree.selected {
			selected = index
			break
		}
	}
	pageSize := max(1, rows-5)
	start := max(0, selected-pageSize+1)
	if start+pageSize > len(nodes) {
		start = max(0, len(nodes)-pageSize)
	}
	end := min(len(nodes), start+pageSize)

	var screen strings.Builder
	fmt.Fprint(&screen, "\x1b[H\x1b[2J")
	fmt.Fprintln(&screen, "Choose where to save received files")
	fmt.Fprintf(&screen, "Selected: %s\n", tree.selected.path)
	fmt.Fprintln(&screen, "Up/Down move  Right expand  Left collapse/go up  Enter select  q cancel")
	for index := start; index < end; index++ {
		node := nodes[index]
		cursor := "  "
		if index == selected {
			cursor = "> "
		}
		marker := "[+] "
		if node.loaded {
			marker = "    "
			if node.expanded {
				marker = "[-] "
			} else if len(node.children) > 0 {
				marker = "[+] "
			}
		}
		label := filepath.Base(node.path)
		if node == tree.root || label == "." || label == string(filepath.Separator) {
			label = node.path
		}
		fmt.Fprintf(&screen, "%s%s%s%s\n", cursor, directoryBranch(node, tree.root), marker, label)
	}
	if status != "" {
		fmt.Fprintln(&screen, status)
	}
	_, err := io.WriteString(output, screen.String())
	if err != nil {
		return fmt.Errorf("draw folder picker: %w", err)
	}
	return nil
}

func runDirectoryPicker(input io.Reader, output io.Writer, tree *directoryTree, rows int) (string, error) {
	status := ""
	for {
		if err := renderDirectoryTree(output, tree, rows, status); err != nil {
			return "", err
		}
		status = ""
		key, err := readDirectoryPickerKey(input)
		if err != nil {
			return "", fmt.Errorf("read folder picker input: %w", err)
		}
		switch key {
		case directoryPickerUp:
			tree.move(-1)
		case directoryPickerDown:
			tree.move(1)
		case directoryPickerRight:
			if err := tree.expand(tree.selected); err != nil {
				status = err.Error()
			}
		case directoryPickerLeft:
			if tree.selected.expanded {
				tree.selected.expanded = false
			} else if tree.selected.parent != nil {
				tree.selected = tree.selected.parent
			} else if err := tree.moveToParent(); err != nil {
				status = err.Error()
			}
		case directoryPickerEnter:
			return tree.selected.path, nil
		case directoryPickerCancel:
			return "", ErrDirectorySelectionCancelled
		}
	}
}

func directoryPickerRootPath(home string) string {
	if volume := filepath.VolumeName(home); volume != "" {
		return volume + string(os.PathSeparator)
	}
	return string(os.PathSeparator)
}

// PickDirectory opens a folder tree when running in an interactive terminal.
func PickDirectory() (string, error) {
	if !term.IsTerminal(int(os.Stdin.Fd())) || !term.IsTerminal(int(os.Stdout.Fd())) {
		return ".", nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("find user folder for picker: %w", err)
	}
	rootPath := directoryPickerRootPath(home)
	if rootPath == "" {
		return "", errors.New("find starting folder for picker: no filesystem root")
	}
	tree, err := newDirectoryTree(rootPath)
	if err != nil {
		return "", err
	}
	state, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		return "", fmt.Errorf("enable folder picker input: %w", err)
	}
	output := colorable.NewColorable(os.Stdout)
	selected, pickerErr := func() (selected string, err error) {
		defer func() {
			if restoreErr := term.Restore(int(os.Stdin.Fd()), state); err == nil && restoreErr != nil {
				err = fmt.Errorf("restore terminal after folder picker: %w", restoreErr)
			}
		}()
		_, rows, sizeErr := term.GetSize(int(os.Stdout.Fd()))
		if sizeErr != nil || rows < 1 {
			rows = 24
		}
		return runDirectoryPicker(os.Stdin, output, tree, rows)
	}()
	if _, err := io.WriteString(output, "\x1b[H\x1b[2J"); err != nil && pickerErr == nil {
		pickerErr = fmt.Errorf("clear folder picker: %w", err)
	}
	if pickerErr != nil {
		return "", pickerErr
	}
	if _, err := fmt.Fprintf(output, "Saving received files to %s\n", selected); err != nil {
		return "", fmt.Errorf("show selected folder: %w", err)
	}
	return selected, nil
}
