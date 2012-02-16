array set recorded {}
proc mkkey {a b n} {
	if {$a > $b} {
		set t $a
		set a $b
		set b $t
	}
	return $a:$b:$n
}
while {![eof stdin]} {
	set l [gets stdin]
	if {[regexp {Registering edge [(]([0-9]+),([0-9]+)[)].*node ([0-9]+)} $l _ a b node]} {
		set key [mkkey $a $b $node]
		set recorded($key) $l
	}
	if {[regexp {Edge [(]([0-9]+),([0-9]+)[)].*(exists|doesn't exist).*node ([0-9]+)} $l _ a b flag node]} {
		set key [mkkey $a $b $node]
		set ourflag "doesn't exist"
		set defined "<not defined>"
		if {[info exists recorded($key)]} {
			set ourflag "exists"
			set defined $recorded($key)
		}
		if {![string equal $flag $ourflag]} {
			puts "mismatch. We think $key $ourflag, program thinks it $flag"
			puts "defined: $defined"
			puts "this line: $l"
		}
	}
	if {[regexp "silen" $l]} {
		puts "\n\n[string repeat - 40]\n\n"
	}
}