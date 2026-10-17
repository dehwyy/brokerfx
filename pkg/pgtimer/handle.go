package pgtimer

import "fmt"

func (p *Poller) Handle(
	name string,
	handler HandlerFunc,
) error {
	if name == "" || handler == nil {
		return fmt.Errorf(
			"%w: handler name and func are required",
			ErrInvalidConfig,
		)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state != stateIdle {
		return ErrStarted
	}
	if _, ok := p.handlers[name]; ok {
		return fmt.Errorf(
			"%w: %s",
			ErrDuplicateHandler,
			name,
		)
	}
	p.handlers[name] = handler

	return nil
}
