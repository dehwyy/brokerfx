package consumeroptsbuilder

import "slices"

func (b *ConsumerOptsBuilder) WithFilterSubjects(
	filterSubjects ...string,
) *ConsumerOptsBuilder {
	b.config.FilterSubject = ""
	b.config.FilterSubjects = slices.Clone(filterSubjects)
	return b
}
