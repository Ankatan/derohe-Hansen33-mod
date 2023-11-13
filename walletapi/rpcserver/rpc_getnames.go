package rpcserver

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/deroproject/derohe/rpc"
)

//import	"log"
//import 	"net/http"

func GetNames(ctx context.Context) (result rpc.GetNames_Result, err error) {
	defer func() { // safety so if anything wrong happens, we return error
		if r := recover(); r != nil {
			err = fmt.Errorf("panic occured. stack trace %s", debug.Stack())
		}
	}()
	w := FromContext(ctx)
	addr := w.wallet.GetAddress().String()
	names, err := w.wallet.AddressToName(addr)
	return rpc.GetNames_Result{
		Names: names,
	}, err
}
