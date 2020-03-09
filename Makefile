xml2rfc ?= xml2rfc
kramdown-rfc2629 ?= kramdown-rfc2629

drafts := draft-schedulers.txt draft-schedulers-mptcp.txt draft-schedulers-mpquic.txt
xml := $(drafts:.txt=.xml)

%.txt: %.mkd
	mdspell -n -a --en-us -r $<
	$(kramdown-rfc2629) $< > $(patsubst %.txt,%.xml, $@)
	$(xml2rfc) $(patsubst %.txt,%.xml, $@) > $@

%.txt: %.xml
	$(xml2rfc) $< $@

%.html: %.xml
	$(xml2rfc) --html $< $@

all: $(drafts)
