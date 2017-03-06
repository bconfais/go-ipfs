package commands

import (
	"time"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/ipfs/go-ipfs/blocks"
	key "github.com/ipfs/go-ipfs/blocks/key"
	cmds "github.com/ipfs/go-ipfs/commands"
	mh "gx/ipfs/QmYf7ng2hG5XBtJA3tN34DQ2GUN5HNksEw1rLDkmr6vGku/go-multihash"
	u "gx/ipfs/QmZNVWh8LLjAavuQ2JXuFmuYH3C11xo988vSgp7UQrTRj1/go-ipfs-util"
)

type BlockStat struct {
	Key  string
	Size int
}

func (bs BlockStat) String() string {
	return fmt.Sprintf("Key: %s\nSize: %d\n", bs.Key, bs.Size)
}

var BlockCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Manipulate raw IPFS blocks.",
		ShortDescription: `
'ipfs block' is a plumbing command used to manipulate raw ipfs blocks.
Reads from stdin or writes to stdout, and <key> is a base58 encoded
multihash.
`,
	},

	Subcommands: map[string]*cmds.Command{
		"stat": blockStatCmd,
		"get":  blockGetCmd,
		"put":  blockPutCmd,
	},
}

var blockStatCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Print information of a raw IPFS block.",
		ShortDescription: `
'ipfs block stat' is a plumbing command for retrieving information
on raw ipfs blocks. It outputs the following to stdout:

	Key  - the base58 encoded multihash
	Size - the size of the block in bytes

`,
	},

	Arguments: []cmds.Argument{
		cmds.StringArg("key", true, false, "The base58 multihash of an existing block to stat.").EnableStdin(),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		b, err := getBlockForKey(req, req.Arguments()[0])
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		res.SetOutput(&BlockStat{
			Key:  b.Key().B58String(),
			Size: len(b.Data()),
		})
	},
	Type: BlockStat{},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			bs := res.Output().(*BlockStat)
			return strings.NewReader(bs.String()), nil
		},
	},
}

var blockGetCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Get a raw IPFS block.",
		ShortDescription: `
'ipfs block get' is a plumbing command for retrieving raw ipfs blocks.
It outputs to stdout, and <key> is a base58 encoded multihash.
`,
	},

	Arguments: []cmds.Argument{
		cmds.StringArg("key", true, false, "The base58 multihash of an existing block to get.").EnableStdin(),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		b, err := getBlockForKey(req, req.Arguments()[0])
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		res.SetOutput(bytes.NewReader(b.Data()))
	},
}

var blockPutCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Stores input as an IPFS block.",
		ShortDescription: `
'ipfs block put' is a plumbing command for storing raw ipfs blocks.
It reads from stdin, and <key> is a base58 encoded multihash.
`,
	},

	Arguments: []cmds.Argument{
		cmds.FileArg("data", true, false, "The data to be stored as an IPFS block.").EnableStdin(),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		fmt.Printf("ok\n")

		s1 := time.Now()
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		ss1 := time.Since(s1)
		fmt.Printf("InvocContext took %s\n", ss1)

		s2 := time.Now()
		file, err := req.Files().NextFile()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		ss2 := time.Since(s2)
		fmt.Printf("Files().NextFile() took %s\n", ss2)

		s3 := time.Now()
		data, err := ioutil.ReadAll(file)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		ss3 := time.Since(s3)
		fmt.Printf("ioutil:ReadAll took %s\n", ss3)

		s4 := time.Now()
		err = file.Close()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		ss4 := time.Since(s4)
		fmt.Printf("close (of readall) took %s\n", ss4)

		s5 := time.Now()
		b := blocks.NewBlock(data)
		log.Debugf("BlockPut key: '%q'", b.Key())
		ss5 := time.Since(s5)
		fmt.Printf("Newblock took %s\n", ss5)
	
		s6 := time.Now()
		k, err := n.Blocks.AddBlock(b)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		ss6 := time.Since(s6)
		fmt.Printf("Addblock took %s\n", ss6)

		res.SetOutput(&BlockStat{
			Key:  k.String(),
			Size: len(data),
		})
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			bs := res.Output().(*BlockStat)
			return strings.NewReader(bs.Key + "\n"), nil
		},
	},
	Type: BlockStat{},
}

func getBlockForKey(req cmds.Request, skey string) (blocks.Block, error) {
	n, err := req.InvocContext().GetNode()
	if err != nil {
		return nil, err
	}

	if !u.IsValidHash(skey) {
		return nil, errors.New("Not a valid hash")
	}

	s1 := time.Now()
	h, err := mh.FromB58String(skey)
	if err != nil {
		return nil, err
	}
	ss1 := time.Since(s1)
	fmt.Printf("decode key took %s\n", ss1)

	k := key.Key(h)
	s2 := time.Now()
	b, err := n.Blocks.GetBlock(req.Context(), k)
	if err != nil {
		return nil, err
	}
	ss2 := time.Since(s2)
	fmt.Printf("Blocks.GetBlock took %s\n", ss2)

	log.Debugf("ipfs block: got block with key: %q", b.Key())
	return b, nil
}
