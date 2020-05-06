<template>
    <graphviz :value="value" />
</template>

<script>
import graphviz from "../components/graphviz"
export default {
    components:{
        graphviz
    },
    data() {
        return {
            autoUpdate: true,
            showSubscribers:true
        };
    },
    computed:{
        value(){
            if(this.autoUpdate){
                let state = this.$store.state
                return `digraph G {
                    bgcolor="transparent";
                    node[shape=box color="cyan" fontcolor="#feeb73"];
                    edge[color="#c52dd0"]
                    current[shape=box3d label="${state.Address}\\ncpu:${state.CPUUsage >> 0}% mem:${state.Memory.Usage >> 0}%"]
                    ${state.Streams.map(s=>'"'+s.StreamPath+'"[shape=folder'+(this.showSubscribers?'':' label="'+s.StreamPath+'('+s.SubscriberInfo.length+')"')+'];\ncurrent->"'+s.StreamPath+'"[arrowhead=tee]\n'+
                    (this.showSubscribers?s.SubscriberInfo.map(sub=>`"${s.StreamPath}"->"${sub.ID}"`).join("\n"):"")).join("\n")}
                    ${Object.keys(state.Children).map(c=>`"${c}"[shape=box3d]\ncurrent->"${c}"`).join("\n")}
                }`
            }
        }
    },
    mounted() {
        let _this = this;
        this.$parent.titleOps = [
            {
                template:
                    "<mu-switch label='自动更新' v-model='value'></mu-swtich>",
                data() {
                    return { value: _this.autoUpdate };
                },
                watch: {
                    value(v) {
                        _this.autoUpdate = v;
                    }
                }
            },
            {
                template:
                    "<mu-checkbox label='显示订阅者' v-model='value'></mu-checkbox>",
                data() {
                    return { value: _this.showSubscribers };
                },
                watch: {
                    value(v) {
                        _this.showSubscribers = v;
                    }
                }
            }
        ];
    }
};
</script>

<style>
</style>