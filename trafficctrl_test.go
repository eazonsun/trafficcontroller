package trafficcontroller

import (
	"fmt"
	"time"
	"testing"
)

var g_trafficCtrl *TrafficController

func init() {
	g_trafficCtrl = new(TrafficController)

	redisCfg := new(RedisCfg)
	redisCfg.Hosts = append(redisCfg.Hosts, "10.231.135.81:6379")
	redisCfg.MaxActive = 200
	redisCfg.MaxIdle   = 200
	redisCfg.IdleTmout = 200

	g_trafficCtrl = CreateTrafficController(redisCfg, 2000)
	g_trafficCtrl.RegisterCaller("rgeo", "257", "TD5Y1-7RHH3-NHP1R-WHI87-8JRFN-986T7", 10, 5)
	g_trafficCtrl.RegisterCaller("rgeo", "258", "QG2GL-88NTJ-KT6MW-0NVZ1-JQC1A-45S0Y", 20, 5)
}

func TestNormal(t *testing.T) {
	timer := time.NewTicker(time.Second * 1)
	quit  := time.NewTicker(time.Second * 120)
	taxi := GenerateKey("rgeo", "257", "TD5Y1-7RHH3-NHP1R-WHI87-8JRFN-986T7")

	LABEL:
	for {
		select {
		case <-timer.C:
			fmt.Println("<<<<< Output >>>>>")
			fmt.Printf("Applied: %d\n",g_trafficCtrl.TrafficCtrl[taxi].AppliedQps)
			fmt.Printf("LastTotalCnt: %d\n", g_trafficCtrl.TrafficCtrl[taxi].LastTotalCnt)
			fmt.Printf("CurrentCnt: %d\n", *(g_trafficCtrl.TrafficCtrl[taxi].CurrentCnt))
			fmt.Printf("Bucket: %d\n", g_trafficCtrl.TrafficCtrl[taxi].Bucket)	
		case <-quit.C:
			break LABEL
		default:
			time.Sleep(time.Millisecond * 500)
			if g_trafficCtrl.IsTrafficCtrl("RgEo", "257", "TD5Y1-7RHH3-NHP1R-WHI87-8JRFN-986T7") == true {
				fmt.Println("Overtraffic the qps configuration to 257")
			} else {
				fmt.Println("Provide service to 257")
			}
		}
	}
	t.Log("TestNormal done")
}

func TestOvertraffic(t *testing.T) {
	timer := time.NewTicker(time.Second * 1)
	quit  := time.NewTicker(time.Second * 120)
	gs := GenerateKey("rGEO", "258", "QG2GL-88NTJ-KT6MW-0NVZ1-JQC1A-45S0Y")
	LABEL:
	for {
		select {
		case <-timer.C:
			fmt.Println("<<<<< Output >>>>>")

			fmt.Printf("Applied: %d\n",g_trafficCtrl.TrafficCtrl[gs].AppliedQps)
			fmt.Printf("LastTotalCnt: %d\n", g_trafficCtrl.TrafficCtrl[gs].LastTotalCnt)
			fmt.Printf("CurrentCnt: %d\n", *(g_trafficCtrl.TrafficCtrl[gs].CurrentCnt))
			fmt.Printf("Bucket: %d\n", g_trafficCtrl.TrafficCtrl[gs].Bucket)	
		case <-quit.C:
			break LABEL
		default:
			time.Sleep(time.Millisecond * 100)
			if g_trafficCtrl.IsTrafficCtrl("rgeo", "258", "QG2GL-88NTJ-KT6MW-0NVZ1-JQC1A-45S0Y") == true {
				fmt.Println("Overtraffic the qps configuration to 258")
			} else {
				fmt.Println("Provide service to 258")
			}
		}
	}
	t.Log("TestOvertraffic done")
}
