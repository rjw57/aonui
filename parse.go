// Parsing functions

package aonui

import "code.google.com/p/go.net/html"

type nodeFunc func(node *html.Node)

// Walk a HTML parse tree in a depth first manner calling nodeFn for each node.
func walkNodeTree(root *html.Node, nodeFn nodeFunc) {
	// Process root
	nodeFn(root)

	// Walk children concurrently
	for c := root.FirstChild; c != nil; c = c.NextSibling {
		walkNodeTree(c, nodeFn)
	}
}
